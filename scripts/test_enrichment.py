import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_geohash_join_logic(spark):
    # Создаем тестовые данные ресторанов
    rest_data = [("Rest1", "u09t"), ("Rest2", "u09t")]
    rest_df = spark.createDataFrame(rest_data, ["name", "geohash"])

    # Создаем тестовые данные погоды (2 записи для одного хеша - риск дублирования!)
    weather_data = [("u09t", 20.0, "2025-01-01"), ("u09t", 22.0, "2025-01-01")]
    weather_df = spark.createDataFrame(weather_data, ["geohash", "avg_temp", "w_date"])

    # Агрегация (наша логика дедупликации)
    weather_agg = weather_df.groupBy("geohash").agg({"avg_temp": "avg"})

    # Join
    result_df = rest_df.join(weather_agg, on="geohash", how="left")

    # Проверка: не должно стать 4 строки вместо 2!
    assert result_df.count() == 2
    assert "avg(avg_temp)" in result_df.columns
    