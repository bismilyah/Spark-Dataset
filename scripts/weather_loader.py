# -*- coding: utf-8 -*-
import os
import zipfile
import shutil

def extract_weather_data(base_path, target_path):
    """
    Распаковывает все zip-архивы из base_path в target_path,
    сохраняя структуру папок year/month/day.
    """
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    print("Searching for zip files in: {}".format(base_path))
    
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                print("Extracting: {}".format(file))
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # Извлекаем во временную папку
                    temp_extract = os.path.join(target_path, "_temp")
                    zip_ref.extractall(temp_extract)
                    
                    # Ищем где внутри распакованного архива лежит папка 'weather'
                    # и переносим её содержимое в основной target_path
                    for weather_root, weather_dirs, weather_files in os.walk(temp_extract):
                        if 'year=' in weather_root:
                            # Определяем относительный путь от папки weather
                            # например: year=2016/month=10/day=01
                            rel_path = weather_root.split('weather' + os.sep)[-1]
                            dest_dir = os.path.join(target_path, rel_path)
                            
                            if not os.path.exists(dest_dir):
                                os.makedirs(dest_dir)
                                
                            for f in weather_files:
                                if f.endswith(".parquet"):
                                    shutil.copy2(os.path.join(weather_root, f), os.path.join(dest_dir, f))
                
                # Чистим временные файлы после каждого архива
                if os.path.exists(os.path.join(target_path, "_temp")):
                    shutil.rmtree(os.path.join(target_path, "_temp"))

    print("All data extracted to: {}".format(target_path))

if __name__ == "__main__":
    # /data - это папка, примонтированная из Windows
    SOURCE = "/data/weather" 
    TARGET = "/data/weather_unpacked"
    extract_weather_data(SOURCE, TARGET)