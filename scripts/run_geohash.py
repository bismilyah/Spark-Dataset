# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, IntegerType

def encode_geohash(latitude, longitude, precision=4):
    if latitude is None or longitude is None:
        return None
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
            geohash.append(base32[ch])
            bit, ch = 0, 0
    return "".join(geohash)

def process():
    spark = SparkSession.builder.appName("GeohashJob") \
        .config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()

    # Схема данных из первого этапа
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

    input_path = "/data/geocoded_result"
    output_path = "/data/restaurant_with_geohash"

    print("Reading geocoded data...")
    # Читаем данные, которые мы только что сохранили
    df = spark.read.schema(schema).parquet(input_path)

    geohash_udf = udf(lambda lat, lng: encode_geohash(lat, lng, 4), StringType())

    print("Generating geohashes...")
    final_df = df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    # Показываем результат
    final_df.select("franchise_name", "city", "geohash").show(10)
    
    print("TOTAL ROWS WITH GEOHASH: {}".format(final_df.count()))

    print("Saving to: {}".format(output_path))
    final_df.write.mode("overwrite").parquet(output_path)
    
    print("Geohash job finished successfully!")
    spark.stop()

if __name__ == "__main__":
    process()
