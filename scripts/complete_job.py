# -*- coding: utf-8 -*-
import json
import urllib2
import urllib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split, when
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, IntegerType

API_KEY = "f01a984e44bf45e49d3266e3719e743c"

def get_coords(city, country, franchise):
    if not city or not country: return None
    try:
        q = "{}, {}, {}".format(franchise, city, country)
        url = "https://api.opencagedata.com/geocode/v1/json?q={}&key={}".format(urllib.quote(q), API_KEY)
        data = json.load(urllib2.urlopen(url))
        if data['results']:
            geom = data['results'][0]['geometry']
            return "{},{}".format(geom['lat'], geom['lng'])
    except: pass
    return None

def encode_geohash(latitude, longitude, precision=4):
    if latitude is None or longitude is None: return None
    base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    geohash, bits, bit, ch, even = [], [16, 8, 4, 2, 1], 0, 0, True
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2.0
            if longitude > mid: ch |= bits[bit]; lon_interval = (mid, lon_interval[1])
            else: lon_interval = (lon_interval[0], mid)
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2.0
            if latitude > mid: ch |= bits[bit]; lat_interval = (mid, lat_interval[1])
            else: lat_interval = (lat_interval[0], mid)
        even = not even
        if bit < 4: bit += 1
        else:
            geohash.append(base32[ch]); bit, ch = 0, 0
    return "".join(geohash)

def run():
    spark = SparkSession.builder.appName("CompleteJob").config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()
    
    schema = StructType([
        StructField("id", LongType(), True), StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True), StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True), StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True), StructField("lng", DoubleType(), True)
    ])

    print("--- 1. READING CSV ---")
    df = spark.read.csv("/data/restaurant_csv", header=True, schema=schema)
    
    print("--- 2. GEOCODING (REST API) ---")
    geo_udf = udf(get_coords, StringType())
    # Исправляем только те, где нет координат
    df_geo = df.withColumn("api_res", when(col("lat").isNull(), geo_udf(col("city"), col("country"), col("franchise_name"))).otherwise(None))
    df_fixed = df_geo.withColumn("lat", when(col("lat").isNull(), split(col("api_res"), ",").getItem(0).cast("double")).otherwise(col("lat"))) \
                     .withColumn("lng", when(col("lng").isNull(), split(col("api_res"), ",").getItem(1).cast("double")).otherwise(col("lng"))) \
                     .drop("api_res")

    print("--- 3. GENERATING GEOHASH ---")
    hash_udf = udf(lambda lt, ln: encode_geohash(lt, ln, 4), StringType())
    final_df = df_fixed.withColumn("geohash", hash_udf(col("lat"), col("lng")))

    print("--- RESULT PREVIEW ---")
    final_df.select("franchise_name", "city", "lat", "lng", "geohash").show(20)
    print("TOTAL RECORDS: {}".format(final_df.count()))

    print("--- SAVING TO PARQUET ---")
    final_df.write.mode("overwrite").parquet("/data/restaurant_final_geohash")
    print("DONE! Check folder: /data/restaurant_final_geohash")
    spark.stop()

if __name__ == "__main__":
    run()