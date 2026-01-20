# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, IntegerType

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
    spark = SparkSession.builder.appName("FinalWeatherJoin") \
        .config("spark.sql.parquet.compression.codec", "gzip") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

    # 1. Загрузка ресторанов
    print("--- Loading geocoded restaurants ---")
    rest_df = spark.read.parquet("/data/restaurant_final_geohash")

    # 2. Загрузка распакованной погоды
    print("--- Loading unpacked weather data ---")
    # Читаем данные. Если Snappy все еще будет ругаться, 
    # попробуем прочитать как csv, но сначала пробуем так:
    weather_raw = spark.read.parquet("/data/weather_unpacked")
    
    # 3. Генерация Geohash для погоды
    geohash_udf = udf(lambda lt, ln: encode_geohash(lt, ln, 4), StringType())
    weather_with_hash = weather_raw.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    # 4. Дедупликация погоды (Агрегация)
    print("--- Deduplicating weather ---")
    weather_dedup = weather_with_hash.groupBy("geohash").agg(
        avg("avg_tmpr_c").alias("avg_temp_c"),
        avg("avg_tmpr_f").alias("avg_temp_f")
    )

    # 5. LEFT JOIN (без принудительного broadcast)
    print("--- Joining datasets ---")
    final_df = rest_df.join(weather_dedup, on="geohash", how="left")

    # 6. Сохранение
    output_path = "/data/enriched_data_partitioned"
    print("--- Saving result ---")
    final_df.write.mode("overwrite") \
        .partitionBy("geohash") \
        .parquet(output_path)

    print("JOB FINISHED SUCCESSFULLY. Count: {}".format(final_df.count()))
    spark.stop()

if __name__ == "__main__":
    run()