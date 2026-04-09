from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Metodo que crea una columna adicional FlightTime tipo datetime en base a los campos DepTime y FlightDate
    la cual contiene la fecha y hora del vuelo, tomando en cuenta la informacion de timezone.
    :param spark: Sesion de spark
    :param df: Dataframe con campos aplanados
    :param fichero_timezones: fichero csv con informacion de timezones.
    :return: Dataframe con columna adicional FlightTime tipo datetime.
    """

    # Antes de empezar el ejercicio 2, debemos unir a los vuelos la zona horaria del aeropuerto de salida del vuelo,
    # utilizando el CSV de timezones.csv y uniéndolo por código IATA (columna Origin de los datos con columna iata_code
    # del CSV), dejando a null los timezones de los aeropuertos que no aparezcan en dicho fichero CSV si los hubiera.
    # Primero deberemos leer dicho CSV infiriendo el esquema e indicando que las columnas contienen encabezados.

    path_timezones = Path(__file__).parent / "resources"/"timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(timezones_df, df.Origin == timezones_df.iata_code, "left")


    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 2 (2 puntos)
    # ----------------------------------------

    # Añadir por la derecha una columna llamada FlightTime de tipo timestamp, a partir de las columnas
    # FlightDate y DepTime. Para ello:
    # (a) añade una columna llamada castedHour (que borraremos más adelante) como resultado de convertir la columna
    # DepTime a string, y aplicarle a la columna de string la función F.lpad para obtener una nueva columna en la
    # que se ha añadido el carácter "0" por la izquierda tantas veces como sea necesario. De ese modo nos
    # aseguramos de que tendrá siempre 4 caracteres.
    # (b) añade la columna FlightTime, de la forma "2023-12-25 20:04:00", concatenando lo siguiente (F.concat(...)):
    #    i. la columna resultante de convertir FlightDate a string. Esto nos dará la parte "2023-12-15"
    #    ii. un objeto columna constante, igual a " " (carácter espacio)
    #    iii. la columna resultante de tomar el substring que empieza en la posición 1 y tiene longitud 2. Revisa
    #         la documentación del mét odo substr de la clase Column, y aplica (F.col(...).substr(...))
    #     iv. un objeto columna constante igual a ":"
    #     v. la columna resultante de tomar el substring que empieza en la posición 3 y tiene longitud 2. Los puntos
    #        iii, iv y v nos darán la parte "20:04:00" como string
    #     vi. Por último, aplica la función cast("timestamp") al objeto columna devuelto por concat:
    #         F.concat(...).cast("timestamp"). Los pasos i a v deben hacerse **en una única transformación**
    # (c) Finalmente, en una nueva transformación, reemplaza la columna FlightTime por el resultado de aplicar la
    #     función F.to_utc_timestamp("columna", "time zone") siendo "columna" la columna FlightTime y siendo
    #     "iana_tz" la columna que contiene la zona horaria en base a la cuál debe interpretarse el timestamp
    #     que ya teníamos en FlightTime
    # (d) Antes de devolver el DF resultante, borra las columnas que estaban en timezones_df, así como la columna
    #     castedHour

    #punto a (transformacion con columna castedHour) y b(campo datetime con FlightTime y castedHour)
    df_with_flight_time = df_with_tz.withColumn(
        "castedHour", F.lpad(F.col("DepTime").cast("string"), 4, "0")
    ).withColumn(
        "FlightTime",
        F.concat(
            F.col("FlightDate").cast("string"),
            F.lit(" "),
            F.col("castedHour").substr(1, 2),
            F.lit(":"),
            F.col("castedHour").substr(3, 2)
        ).cast("timestamp")
    )

    # c (Conversión a UTC)
    df_with_flight_time = df_with_flight_time.withColumn(
        "FlightTime", F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz"))
    )

    # d (Limpieza de columnas auxiliares y las del timezones_df)
    cols_to_drop = timezones_df.columns + ["castedHour"]

    return df_with_flight_time.drop(*cols_to_drop)


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Completa la documentación
    :param df:
    :return:
    """
    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 3 (2 puntos)
    # ----------------------------------------

    # Queremos pegarle a cada vuelo la información del vuelo que despega justo después de su **mismo
    # aeropuerto de origen**. En concreto queremos saber la hora de despegue del siguiente vuelo y la compañía aérea.
    # Para ello, primero crea una columna de pares (FlightTime, Reporting_Airline), y después crea otra columna
    # adicional utilizando la función F.lag(..., -1) con dicha columna, dentro de una ventana que
    # debe estar particionada adecuadamente y ordenada adecuadamente. No debes utilizar la transformación sort()
    # de los DF. Después, extrae los dos campos internos de la tupla como columnas llamadas "FlightTime_next" y "Airline_next",
    # y calcula una nueva columna diff_next con la diferencia en segundos entre la hora de salida de un vuelo y la
    # del siguiente, como la diferencia de ambas columnas (next menos actual) tras haberlas convertido al tipo "long".
    # El DF resultante de esta función debe ser idéntico al de entrada pero con 3 columnas nuevas añadidas por la
    # derecha, llamadas FlightTime_next, Airline_next y diff_next. Cualquier columna auxiliar debe borrarse.

    #ventana particionada por campo origin, ordenada por FlightTime
    w = Window.partitionBy("Origin").orderBy("FlightTime")

    # columna info_vuelo de pares FlightTime, Reporting_Airline
    df_with_next_flight = df.withColumn("info_vuelo", F.struct("FlightTime", "Reporting_Airline"))

    # columna vuelo_siguiente usando usando ventana tomando como referencia campo info_vuelo
    df_with_next_flight = df_with_next_flight.withColumn("vuelo_siguiente", F.lag(F.col("info_vuelo"), -1).over(w))

    # añadir info en la estructura de FlightTime y Reporting_Airline para vuelo_siguiente
    df_with_next_flight = df_with_next_flight.withColumn(
        "FlightTime_next", F.col("vuelo_siguiente.FlightTime")
    ).withColumn(
        "Airline_next", F.col("vuelo_siguiente.Reporting_Airline")
    )

    # calcular la diferencia entre FlightTime_next y FlightTime convirtiendo a long
    df_with_next_flight = df_with_next_flight.withColumn(
        "diff_next",
        F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long")
    )
    # eliminar columnas auxiliares info_vuelo y vuelo_siguiente
    return df_with_next_flight.drop("info_vuelo","vuelo_siguiente")
