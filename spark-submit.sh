#!/bin/bash

# --- Конфигурация Job ---
INPUT_PATH="/data/restaurant_csv"      
OUTPUT_PATH="/tmp/spark_output/restaurant_counts"
SCRIPT_FILE="/app/data_processor.py"
MASTER_URL="spark://spark-master:7077" 

# --- Запуск Job ---
echo "--- Starting Spark Job ---"
echo "Input Path: ${INPUT_PATH}"
echo "Output Path: ${OUTPUT_PATH}"

# Запускаем Spark Job ОДНОЙ длинной строкой для надежности
/spark/bin/spark-submit \
  --master ${MASTER_URL} \
  --deploy-mode client \
  --name "RestaurantAnalysis_Optimized" \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  --conf spark.executor.memoryOverhead=512m \
  --py-files ${SCRIPT_FILE} \
  ${SCRIPT_FILE} \
  ${INPUT_PATH} \
  ${OUTPUT_PATH}

echo "--- Spark Job command finished ---"