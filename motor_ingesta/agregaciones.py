from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Metodo que crea una columna adicional FlightTime tipo datetime en base a los campos DepTime y FlightDate
    la cual contiene la fecha y hora del vuelo, del dataframe de vuelos, tomando ademas la informacion de timezone.
    :param spark: Sesion de spark
    :param df: Dataframe de vuelos con campos aplanados
    :param fichero_timezones: fichero csv con informacion de timezones.
    :return: Dataframe con columna adicional FlightTime tipo datetime.
    """

    # Uso de CSV de timezones.csv y union por código IATA con dataframe de vuelos
    path_timezones = Path(__file__).parent / "resources"/"timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(timezones_df, df.Origin == timezones_df.iata_code, "left")

    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 2
    # ----------------------------------------

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
    Genera la informacion de conexiones de los vuelos posteriores para cada vuelo registrado en el dataframe.
    :param df: dataframe de vuelos con columna FlightTime
    :return: Dataframe de vuelos con columnas adicionales FlightTime_next, Airline_next y diff_next
    """
    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 3
    # ----------------------------------------


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
