import json
from datetime import timedelta
from loguru import logger
from pathlib import Path
import os


from pyspark.sql import functions as F

from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from motor_ingesta.motor_ingesta import MotorIngesta


class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Constructor que inicializa la configuración y la sesión de Spark adecuada.
        :param config_file:
        """
        # Leer como diccionario el fichero json indicado en la ruta config_file, usando json.load(f) del paquete json
        # y almacenarlo en self.config.

        with open(config_file, "r") as f:
            self.config = json.load(f)

        # Además, crear la SparkSession si no existiese usando
        # SparkSession.builder.getOrCreate() que devolverá la sesión existente, o creará una nueva si no existe ninguna

        #evalua la variable entorno de ejecucion para saber si debe crear una DatabricksSession o una SparkSession
        if self.config.get("EXECUTION_ENVIRONMENT") == "databricks":
            from databricks.connect import DatabricksSession
            self.spark = DatabricksSession.builder.getOrCreate()
        else:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.getOrCreate()


    def procesa_diario(self, data_file: str):
        """
        Orquesta el proceso de ingesta, transformación y persistencia de inforamcion de vuelos creando un nuevo objeto
        motor de ingesta, aplica transformaciones y guarda los datos en una tabla agestionada.
        :param data_file: Ruta donde se encuentra el fichero json con la informacion de vuelos
        """


        try:

            # objeto motor_ingesta que invoca metodo ingesta_fichero
            motor_ingesta = MotorIngesta(self.config, spark=self.spark)
            flights_df = motor_ingesta.ingesta_fichero(data_file)

            # Cacheo flights_df para evitar reprocesar el json en cada acción
            flights_df.cache()

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = aniade_hora_utc(self.spark, flights_df)


            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == dia_previo)
                flights_previo.limit(1).collect() # para verificar que realmente busca en la tabla
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:


                cols_vuelo_siguiente = ["FlightTime_next", "Airline_next", "diff_next"]
                df_hoy_preparado = flights_with_utc
                for col in cols_vuelo_siguiente:
                    df_hoy_preparado = df_hoy_preparado.withColumn(col, F.lit(None))

                df_unido = flights_previo.select(df_hoy_preparado.columns).union(df_hoy_preparado)

                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # Escribimos el DF en la tabla externa config["output_table"], sera una tabla gestionada
            # por tanto no se usa ouyput path, con el número de particiones indicado en config["output_partitions"]
            # df_with_next_flight.....(...)..write.mode("overwrite").option("partitionOverwriteMode", "dynamic")....
            df_with_next_flight\
                .coalesce(self.config.get("output_partitions", 1))\
                .write.mode("overwrite").option("partitionOverwriteMode", "dynamic")\
                .partitionBy("FlightDate")\
                .saveAsTable(self.config["output_table"])

            logger.info(f"Procesamiento completado para el día {dia_actual}")


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
   # spark = SparkSession.builder.getOrCreate()   # sólo si lo ejecutas localmente
    root_path = Path(__file__).parent.parent
    path_config = str(root_path/"config"/"config.json")
    flujo = FlujoDiario(path_config)
    flujo.procesa_diario("abfss://datos@masterap001sta.dfs.core.windows.net/2023-01-01.json")

    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel
