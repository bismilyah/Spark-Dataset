# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def create_spark_session(app_name):
    # Используем .format(), так как f-строки тоже не работают в Python 2
    print("Initializing Spark session for application: {}".format(app_name))

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "200") 
        .config("spark.sql.parquet.compression.codec", "gzip") 
        .getOrCreate()
    )
    return spark

# УДАЛЕНЫ аннотации типов : SparkSession, : str
def process_data(spark, input_path, output_path):
    """
    Логика PySpark Job: чтение, обработка и запись данных.
    """
    
    print("Reading data from: {}".format(input_path))
    df = (
        spark.read.csv(
            input_path, 
            header=True,
            inferSchema=True
        )
    )
    
    print("Starting data aggregation...")

    # Посмотрим, прочитались ли данные вообще
    print("Input data preview:")
    df.show(5) 
    print("Total rows read: {}".format(df.count()))

    result_df = (
        df.filter(col("country").isNotNull())
        .groupBy("franchise_name", "city")
        .agg(count(col("id")).alias("restaurant_count"))
        .orderBy(col("restaurant_count").desc(), col("franchise_name"))
    )
    # Посмотрим результат перед записью
    print("Result data preview:")
    result_df.show(5)
    
    print("Writing result to: {}".format(output_path))
    result_df.write.mode("overwrite").parquet(output_path)
    
    print("Job completed successfully! Results are saved.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit data_processor.py <input_data_path> <output_data_path>")
        sys.exit(-1)

    INPUT_PATH = sys.argv[1]
    OUTPUT_PATH = sys.argv[2]
    
    APP_NAME = "RestaurantAnalysisJob"
    
    try:
        spark = create_spark_session(APP_NAME)
        process_data(spark, INPUT_PATH, OUTPUT_PATH)
    finally:
        # В Python 2 проверка на существование переменной через locals()
        if 'spark' in locals() and spark is not None:
            spark.stop()