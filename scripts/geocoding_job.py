# -*- coding: utf-8 -*-
import sys
import json
import urllib2
import urllib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# 🌟 ВАШ КЛЮЧ OPENCAGE 🌟
OPENCAGE_API_KEY = "f01a984e44bf45e49d3266e3719e743c"
def get_coords_from_api(city, country, franchise_name):
    if city is None or country is None:
        return None
    query = "{}, {}, {}".format(franchise_name, city, country)
    url = "https://api.opencagedata.com/geocode/v1/json?q={}&key={}".format(urllib.quote(query), OPENCAGE_API_KEY)
    try:
        response = urllib2.urlopen(url)
        data = json.load(response)
        if data['results']:
            geometry = data['results'][0]['geometry']
            return "{},{}".format(geometry['lat'], geometry['lng'])
    except:
        return None
    return None

def process():
    spark = SparkSession.builder \
        .appName("GeocodingJob") \
        .config("spark.sql.parquet.compression.codec", "gzip") \
        .getOrCreate()

    # Схема входных данных
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ])

    # 1. Читаем исходные CSV (они лежат в примонтированной папке /data)
    input_path = "/data/restaurant_csv"
    output_path = "/data/geocoded_result"

    print("Reading data from: {}".format(input_path))
    df = spark.read.csv(input_path, header=True, schema=schema)

    # 2. Фильтруем пустые координаты
    bad_df = df.filter(col("lat").isNull() | col("lng").isNull())
    good_df = df.filter(col("lat").isNotNull() & col("lng").isNotNull())

    print("Rows to fix: {}".format(bad_df.count()))

    if bad_df.count() > 0:
        geocode_udf = udf(get_coords_from_api, StringType())
        fixed_bad_df = bad_df.withColumn("api_res", geocode_udf(col("city"), col("country"), col("franchise_name")))
        
        fixed_bad_df = fixed_bad_df.withColumn("lat", split(col("api_res"), ",").getItem(0).cast("double")) \
                                   .withColumn("lng", split(col("api_res"), ",").getItem(1).cast("double")) \
                                   .drop("api_res")

        final_df = good_df.union(fixed_bad_df)
    else:
        final_df = good_df

    # 3. Сохраняем результат в ПАПКУ /DATA (чтобы воркеры и мастер его видели)
    print("Saving enriched data to: {}".format(output_path))
    final_df.write.mode("overwrite").parquet(output_path)
    
    # 4. Проверка: выводим строку, которая была пустой (Savoria)
    print("Checking fixed row (Savoria):")
    final_df.filter(col("franchise_name") == "Savoria").show()

    print("Job finished successfully!")
    spark.stop()

if __name__ == "__main__":
    process()



    