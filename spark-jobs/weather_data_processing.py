import os, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():

        # —────────────── CONFIG ─────────────────────────────────────────
    project      = "project-7d3bb007-d066-426a-b52"
    dataset      = "wether_forcasting"
    table        = "weather_data"
    temp_bucket  = "wether_forcasting_rawzone"
    bucket       = "wether_forcasting_rawzone"
    today        = datetime.date.today().strftime("%Y-%m-%d")
    input_path   = f"gs://{bucket}/weather/{today}/forecast.csv"

    # —──────────── SPARK SESSION ───────────────────────────────────
    spark = (
        SparkSession.builder
          .appName("WeatherDataProcessing")
          .getOrCreate()
    )

    df = (
        spark.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").option("quote", '"').csv(input_path)
    )

    # —────────── CAST & RENAME ─────────────────────────────────────
    df2 = (
        df
        .withColumn("dt", from_unixtime(col("dt")).cast("timestamp"))
        .withColumn("dt_txt", to_timestamp(col("dt_txt"), "yyyy-MM-dd HH:mm:ss"))
        .select(
            col("dt").alias("dt"),
            col("dt_txt").alias("forecast_time"),
            col("weather").alias("weather"),
            col("visibility").cast("int").alias("visibility"),
            col("pop").cast("double").alias("pop"),
            col("`main.temp`").cast("double").alias("temp"),
            col("`main.feels_like`").cast("double").alias("feels_like"),
            col("`main.temp_min`").cast("double").alias("min_temp"),
            col("`main.temp_max`").cast("double").alias("max_temp"),
            col("`main.pressure`").cast("long").alias("pressure"),
            col("`main.sea_level`").cast("long").alias("sea_level"),
            col("`main.grnd_level`").cast("long").alias("ground_level"),
            col("`main.humidity`").cast("long").alias("humidity"),
            col("`main.temp_kf`").cast("double").alias("temp_kf"),
            col("`clouds.all`").cast("long").alias("clouds_all"),
            col("`wind.speed`").cast("double").alias("wind_speed"),
            col("`wind.deg`").cast("long").alias("wind_deg"),
            col("`wind.gust`").cast("double").alias("wind_gust"),
            col("`sys.pod`").alias("sys_pod"),
            col("`rain.3h`").cast("double").alias("rain_3h"),
        )
    )

    # —──────────── WRITE TO BQ ─────────────────────────────────────
    (
        df2.write
           .format("bigquery")
           .option("table", f"{project}.{dataset}.{table}")
           .option("temporaryGcsBucket", temp_bucket)
           .option("createDisposition", "CREATE_IF_NEEDED")
           .option("writeDisposition", "WRITE_APPEND")
           .save()
    )

    spark.stop()

if __name__ == "__main__":
    main()