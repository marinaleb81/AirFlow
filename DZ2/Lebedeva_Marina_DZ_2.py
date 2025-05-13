from __future__ import annotations
import json
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
import csv

JSON_SOURCE = '/opt/airflow/dags/data/pets-data.json'
XML_SOURCE = '/opt/airflow/dags/data/nutrition.xml'
DATASET_PATH = '/opt/airflow/dags/data/IOT-temp.csv'
OUTPUT_TRANSFORMED_DATA_PATH = '/opt/airflow/dags/data/transformed_weather.csv'

DATE_COLUMN = 'noted_date'
TEMP_COLUMN = 'temp'
IN_OUT_COLUMN = 'out/in'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pd.Timedelta('5 minutes'),
}

@dag(
    dag_id='Lebedeva_Marina_DZ_2',
    default_args=default_args,
    description='Homework 2: Data Sources and Transformation',
    schedule=None,
    start_date=datetime(2023, 10, 26),
    catchup=False,
    tags=['homework', 'data_engineering', 'transformation'],
)
def data_transformation_homework_dag():

    @task
    def process_json_data(json_source: str):
        print(f"Читаю JSON: {json_source}")
        try:
            with open(json_source, 'r', encoding='utf-8') as f:
                data = json.load(f)

            flat_data = []
            if isinstance(data, dict) and 'pets' in data and isinstance(data['pets'], list):
                print(f"Нашел {len(data['pets'])} записей")
                for pet in data['pets']:
                    if isinstance(pet, dict):
                        flat_pet = pet.copy()

                        if 'favFoods' in flat_pet:
                             if isinstance(flat_pet['favFoods'], list):
                                flat_pet['favFoods'] = ', '.join(map(str, flat_pet['favFoods']))
                             else:
                                 flat_pet['favFoods'] = str(flat_pet['favFoods'])

                        flat_data.append(flat_pet)
                    else:
                        print(f"Пропускаю элемент: {pet}")
                print(f"Получил {len(flat_data)} плоских записей.")
            else:
                print("Неверная структура JSON")

            output_json_csv_path = '/opt/airflow/dags/data/flattened_pets.csv'
            if flat_data:
                all_keys = set().union(*(d.keys() for d in flat_data))
                try:
                    with open(output_json_csv_path, 'w', newline='', encoding='utf-8') as output_file:
                        dict_writer = csv.DictWriter(output_file, fieldnames=sorted(list(all_keys)))
                        dict_writer.writeheader()
                        dict_writer.writerows(flat_data)
                    print(f"Сохранил в {output_json_csv_path}")
                except Exception as save_error:
                    print(f"Ошибка сохранения: {save_error}")
            else:
                 print("Нет данных для сохранения.")


            return json.loads(json.dumps(flat_data, default=str))


        except FileNotFoundError:
            print(f"Ошибка: файл не найден {json_source}")
            raise
        except json.JSONDecodeError as e:
            print(f"Ошибка чтения JSON: {e}")
            raise
        except Exception as e:
            print(f"Ошибка JSON: {e}")
            raise

    @task
    def process_xml_data(xml_source: str):
        print(f"Читаю XML: {xml_source}")
        try:
            tree = ET.parse(xml_source)
            root = tree.getroot()

            flat_data = []
            food_items = root.findall('food')
            print(f"Нашел {len(food_items)} продуктов.")

            for food_elem in food_items:
                food_data = {}

                for child in food_elem:
                    if child.tag in ['vitamins', 'minerals', 'calories', 'serving']:
                        continue
                    food_data[child.tag] = child.text if child.text is not None else ''

                calories_elem = food_elem.find('calories')
                if calories_elem is not None:
                    food_data['calories_total'] = calories_elem.get('total', '')
                    food_data['calories_fat'] = calories_elem.get('fat', '')

                serving_elem = food_elem.find('serving')
                if serving_elem is not None:
                    food_data['serving'] = serving_elem.text if serving_elem.text is not None else ''
                    food_data['serving_units'] = serving_elem.get('units', '')

                vitamins_elem = food_elem.find('vitamins')
                if vitamins_elem is not None:
                     for vita_child in vitamins_elem:
                          food_data[f'vitamins_{vita_child.tag}'] = vita_child.text if vita_child.text is not None else ''

                minerals_elem = food_elem.find('minerals')
                if minerals_elem is not None:
                     for mineral_child in minerals_elem:
                          food_data[f'minerals_{mineral_child.tag}'] = mineral_child.text if mineral_child.text is not None else ''

                if food_data:
                    flat_data.append(food_data)

            print(f"Получил {len(flat_data)} плоских записей.")

            output_xml_csv_path = '/opt/airflow/dags/data/flattened_nutrition.csv'
            if flat_data:
                all_keys = set().union(*(d.keys() for d in flat_data))
                try:
                    with open(output_xml_csv_path, 'w', newline='', encoding='utf-8') as output_file:
                        dict_writer = csv.DictWriter(output_file, fieldnames=sorted(list(all_keys)))
                        dict_writer.writeheader()
                        dict_writer.writerows(flat_data)
                    print(f"Сохранил в {output_xml_csv_path}")
                except Exception as save_error:
                     print(f"Ошибка сохранения: {save_error}")
            else:
                 print("Нет данных для сохранения.")


            return flat_data

        except FileNotFoundError:
            print(f"Ошибка: файл не найден {xml_source}")
            raise
        except ET.ParseError as e:
             print(f"Ошибка чтения XML: {e}")
             raise
        except Exception as e:
            print(f"Ошибка XML: {e}")
            raise


    @task
    def analyze_weather_data(dataset_path: str, date_col: str, temp_col: str):
        print(f"Анализирую погоду: {dataset_path}")
        try:
            df = pd.read_csv(dataset_path)
            df[date_col] = pd.to_datetime(df[date_col], format='%d-%m-%Y %H:%M')

            autumn_df = df[(df[date_col].dt.month >= 9) & (df[date_col].dt.month <= 11)].copy()

            if autumn_df.empty:
                print("Нет данных за осень.")
                return {}

            autumn_df[temp_col] = pd.to_numeric(autumn_df[temp_col], errors='coerce')
            autumn_df_cleaned = autumn_df.dropna(subset=[temp_col])

            if autumn_df_cleaned.empty:
                 print("Нет валидных температур за осень.")
                 return {}

            hottest_days = autumn_df_cleaned.nlargest(5, temp_col)
            print("\n5 самых жарких дней:")
            print(hottest_days[[date_col, temp_col]])

            coldest_days = autumn_df_cleaned.nsmallest(5, temp_col)
            print("\n5 самых холодных дней:")
            print(coldest_days[[date_col, temp_col]])

            hottest_list = hottest_days[[date_col, temp_col]].to_dict('records')
            coldest_list = coldest_days[[date_col, temp_col]].to_dict('records')

            for item in hottest_list:
                 if isinstance(item.get(date_col), pd.Timestamp):
                      item[date_col] = item[date_col].isoformat()
            for item in coldest_list:
                 if isinstance(item.get(date_col), pd.Timestamp):
                      item[date_col] = item[date_col].isoformat()


            return {
                'hottest_days': hottest_list,
                'coldest_days': coldest_list
            }

        except FileNotFoundError:
            print(f"Ошибка: файл не найден {dataset_path}")
            raise
        except KeyError as e:
            print(f"Ошибка: колонка не найдена '{e}'")
            raise
        except Exception as e:
            print(f"Ошибка анализа погоды: {e}")
            raise

    @task
    def transform_weather_data(dataset_path: str, output_path: str, date_col: str, temp_col: str, in_out_col: str):
        print(f"Преобразую погоду: {dataset_path} -> {output_path}")
        try:
            df = pd.read_csv(dataset_path)

            print(f"Фильтрую по '{in_out_col}' = 'in'")
            initial_rows = len(df)

            df[in_out_col] = df[in_out_col].astype(str)

            df_filtered = df[df[in_out_col].str.strip().str.lower() == 'in'].copy()

            print(f"Осталось {len(df_filtered)} из {initial_rows} строк после фильтрации.")

            if df_filtered.empty:
                 print("Нет строк после фильтрации. Создаю пустой файл.")
                 pd.DataFrame(columns=df.columns).to_csv(output_path, index=False)
                 print(f"Сохранил пустой файл: {output_path}")
                 return

            print(f"Форматирую даты в '{date_col}'")
            df_filtered[date_col] = pd.to_datetime(df_filtered[date_col], format='%d-%m-%Y %H:%M')
            df_filtered[date_col] = df_filtered[date_col].dt.strftime('%Y-%m-%d')

            print(f"Очищаю температуру в '{temp_col}' по 5% и 95%")
            df_filtered[temp_col] = pd.to_numeric(df_filtered[temp_col], errors='coerce')
            df_cleaned_temp = df_filtered.dropna(subset=[temp_col]).copy()

            if df_cleaned_temp.empty:
                 print("Нет валидных температур после очистки. Создаю пустой файл.")
                 pd.DataFrame(columns=df.columns).to_csv(output_path, index=False)
                 print(f"Сохранил пустой файл: {output_path}")
                 return

            lower_bound = df_cleaned_temp[temp_col].quantile(0.05)
            upper_bound = df_cleaned_temp[temp_col].quantile(0.95)

            print(f"Процентили температуры: 5% = {lower_bound:.2f}, 95% = {upper_bound:.2f}")

            rows_before_temp_cleaning = len(df_cleaned_temp)
            df_cleaned = df_cleaned_temp[(df_cleaned_temp[temp_col] >= lower_bound) & (df_cleaned_temp[temp_col] <= upper_bound)].copy()

            print(f"Осталось {len(df_cleaned)} из {rows_before_temp_cleaning} строк после очистки.")

            print(f"Сохраняю в {output_path}")
            df_cleaned.to_csv(output_path, index=False)
            print("Готово!")

        except FileNotFoundError:
            print(f"Ошибка: файл не найден {dataset_path}")
            raise
        except KeyError as e:
            print(f"Ошибка: колонка не найдена '{e}'")
            raise
        except Exception as e:
            print(f"Ошибка преобразования: {e}")
            raise

    process_json_task = process_json_data(json_source=JSON_SOURCE)

    process_xml_task = process_xml_data(xml_source=XML_SOURCE)

    analyze_weather_task = analyze_weather_data(
        dataset_path=DATASET_PATH,
        date_col=DATE_COLUMN,
        temp_col=TEMP_COLUMN
    )
    transform_weather_task = transform_weather_data(
        dataset_path=DATASET_PATH,
        output_path=OUTPUT_TRANSFORMED_DATA_PATH,
        date_col=DATE_COLUMN,
        temp_col=TEMP_COLUMN,
        in_out_col=IN_OUT_COLUMN
    )

    [process_json_task, process_xml_task] >> analyze_weather_task >> transform_weather_task


data_transformation_homework_dag()
