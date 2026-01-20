
# Restaurant and Weather Data Enrichment Project

## Описание проекта
[cite_start]Данный проект реализует конвейер обработки данных (data pipeline) с использованием **PySpark** для очистки, обогащения и объединения данных о ресторанах и погодных условиях[cite: 1, 3].

### Основные задачи:
* [cite_start]**Геокодинг**: Исправление отсутствующих координат (latitude/longitude) с помощью OpenCage API[cite: 7].
* [cite_start]**Feature Engineering**: Генерация 4-символьного Geohash для каждой локации[cite: 8].
* [cite_start]**Интеграция**: Выполнение Left Join между данными о погоде и ресторанах с предотвращением дублирования строк[cite: 9, 26].
* [cite_start]**Оптимизация**: Хранение данных в формате Parquet с партиционированием по geohash[cite: 10, 31].

## Технологический стек
* [cite_start]**Runtime**: Python 2.7 / Spark 2.4.0[cite: 13].
* [cite_start]**Инфраструктура**: Docker-кластер (Master и Worker)[cite: 12].
* [cite_start]**Хранилище**: Локальная файловая система, формат Parquet + Gzip[cite: 14, 30].
* [cite_start]**API**: OpenCage Geocoding[cite: 7].

## Как запустить
1.  Клонируйте репозиторий.
2.  Разверните инфраструктуру:
    ```bash
    docker-compose up -d
    ```
3.  Запустите загрузчик данных:
    ```bash
    python scripts/weather_loader.py
    ```
4.  Запустите основную задачу Spark:
    ```bash
    spark-submit scripts/weather_join_job_v2.py
    ```

## Этапы обработки
1.  [cite_start]**Геокодинг и Geohashing**: Идентификация строк с пропущенными координатами и их заполнение через REST API[cite: 17]. [cite_start]Генерация Geohash с использованием UDF (Base32)[cite: 21].
2.  [cite_start]**Дедупликация погоды**: Агрегация данных о погоде по Geohash с использованием `AVG()`, чтобы избежать размножения строк при Join[cite: 26].
3.  **Join и сохранение**: Выполнение Left Join для сохранения всех данных о ресторанах. [cite_start]Итоговые данные партиционируются по столбцу `geohash`[cite: 28, 31].

## Результаты
[cite_start]Данные успешно сохранены в директорию `/data/enriched_data_partitioned/`[cite: 34].

### Скриншоты выполнения:
* **Вывод Geocoding Job**:
    ![Geocoding Job](screenshots/screenshot_geocoding.png)
* **Результаты тестов (PyTest)**:
    ![PyTest Results](screenshots/screenshot_pytest.png)