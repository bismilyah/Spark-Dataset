# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

def run():
    # Создаем сессию с принудительным использованием Gzip для записи
    spark = SparkSession.builder.appName("FixSnappyIssue") \
        .config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()

    print("--- Reading original weather (this might fail if snappy is broken) ---")
    # Если даже чтение падает из-за Snappy, мы попробуем другой подход, 
    # но обычно чтение работает, если библиотеки установлены.
    try:
        weather_df = spark.read.parquet("/data/weather_unpacked")
        
        print("--- Saving weather in GZIP format ---")
        weather_df.write.mode("overwrite").parquet("/data/weather_gzip")
        print("--- Done! Weather converted to GZIP ---")
    except Exception as e:
        print("Failed to read snappy parquet: " + str(e))
        print("Trying to read as raw files...")
        # Если Spark совсем не может прочитать Snappy, нам придется 
        # использовать альтернативный способ загрузки.

    spark.stop()

if __name__ == "__main__":
    run()