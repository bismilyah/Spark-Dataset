# -*- coding: utf-8 -*-
import sys
import json
import urllib2
import urllib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

OPENCAGE_API_KEY = "f01a984e44bf45e49d3266e3719e743c"

def get_coords(city, country, franchise):
    if not city or not country: return None
    try:
        query = "{}, {}, {}".format(franchise, city, country)
        url = "https://api.opencagedata.com/geocode/v1/json?q={}&key={}".format(urllib.quote(query), OPENCAGE_API_KEY)
        data = json.load(urllib2.urlopen(url))
        if data['results']:
            res = data['results'][0]['geometry']
            return "{},{}".format(res['lat'], res['lng'])
    except: pass
    return None

def process():
    spark = SparkSession.builder.appName("FixGeocoding").config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()
    
    schema = StructType([
        StructField("id", LongType(), True), StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True), StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True), StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True), StructField("lng", DoubleType(), True)
    ])

    df = spark.read.csv("/data/restaurant_csv", header=True, schema=schema)
    print("TOTAL ROWS IN CSV: {}".format(df.count()))

    # Находим строки без координат
    geocode_udf = udf(get_coords, StringType())
    
    # Исправляем только те, где lat IS NULL
    enriched_df = df.withColumn("api_data", 
        when(col("lat").isNull(), geocode_udf(col("city"), col("country"), col("franchise_name")))
        .otherwise(None))

    # Раскладываем координаты обратно в lat/lng если они пришли из API
    final_df = enriched_df.withColumn("lat", 
        when(col("lat").isNull(), split(col("api_data"), ",").getItem(0).cast("double"))
        .otherwise(col("lat"))) \
        .withColumn("lng", 
        when(col("lng").isNull(), split(col("api_data"), ",").getItem(1).cast("double"))
        .otherwise(col("lng"))) \
        .drop("api_data")

    print("ROWS BEFORE SAVING: {}".format(final_df.count()))
    
    # Сохраняем результат
    final_df.write.mode("overwrite").parquet("/data/geocoded_result")
    print("SAVED TO /data/geocoded_result")
    spark.stop()

if __name__ == "__main__":
    process()