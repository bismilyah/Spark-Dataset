# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, broadcast
from pyspark.sql.types import StringType

# Используем ту же функцию хеширования, что и для ресторанов
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
    spark = SparkSession.builder.appName("WeatherJoin") \
        .config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()

    # 1. Загрузка данных
    print("Loading restaurant data...")
    rest_df = spark.read.parquet("/data/restaurant_with_geohash")

    print("Loading weather data...")
    # Загружаем погоду (CSV)
    weather_raw = spark.read.csv("/data/weather_csv", header=True, inferSchema=True)

    # 2. Генерация Geohash для погоды
    print("Generating geohash for weather...")
    geohash_udf = udf(lambda lt, ln: encode_geohash(lt, ln, 4), StringType())
    weather_with_hash = weather_raw.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    # 3. ДЕДУПЛИКАЦИЯ: Группируем по хешу и дате
    # Это гарантирует: 1 geohash + 1 дата = 1 строка с погодой
    print("Deduplicating weather data...")
    weather_dedup = weather_with_hash.groupBy("geohash", "w_date").agg(
        avg("avg_tmpr_c").alias("avg_temp_c"),
        avg("avg_tmpr_f").alias("avg_temp_f")
    )

    # 4. LEFT JOIN
    # Рестораны слева, погода справа. 
    # Если погоды нет для этого места/даты, данные ресторана останутся, а погода будет null.
    print("Performing Left Join...")
    final_df = rest_df.join(broadcast(weather_dedup), on="geohash", how="left")

    # 5. Сохранение с партиционированием
    print("Saving enriched data with partitioning...")
    # Партиционируем по geohash для быстрой фильтрации в будущем
    final_df.write.mode("overwrite").partitionBy("geohash").parquet("/data/enriched_data_final")

    print("SUCCESS! Final count: {}".format(final_df.count()))
    final_df.show(10)
    spark.stop()

if __name__ == "__main__":
    run()
    