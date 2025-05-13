from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
import pandas as pd
from datetime import timedelta, datetime 
import os

SOURCE_CSV_PATH = '/opt/airflow/dags/data/transformed_weather.csv'
OUTPUT_DWH_DIR = '/opt/airflow/dags/output_dwh'

os.makedirs(OUTPUT_DWH_DIR, exist_ok=True)

@dag(
    dag_id='Lebedeva_Marina_DZ_3_FullLoad',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dwh_file', 'full_load', 'homework3', 'transformed_weather']
)
def full_load_transformed_weather_to_file_dag():
    @task
    def read_source_data() -> pd.DataFrame:
        print(f"Читаю: {SOURCE_CSV_PATH}")
        return pd.read_csv(SOURCE_CSV_PATH)

    @task
    def write_full_load_to_csv(df: pd.DataFrame):
        output_file = os.path.join(OUTPUT_DWH_DIR, 'transformed_weather_full_load.csv')
        print(f"Пишу полную выгрузку в: {output_file}")
        df.to_csv(output_file, index=False)
        print(f"Полная выгрузка завершена. Сохранено строк: {len(df)}")

    source_dataframe = read_source_data()
    write_full_load_to_csv(source_dataframe)

@dag(
    dag_id='Lebedeva_Marina_DZ_3_IncrementalLoad',
    schedule='@daily', 

    start_date=pendulum.datetime(2018, 11, 1, tz="UTC"), 
    catchup=False,
    tags=['dwh_file', 'incremental_load', 'homework3', 'transformed_weather']
)
def incremental_load_transformed_weather_to_file_dag():
    @task
    def read_source_data_for_increment() -> pd.DataFrame:
        print(f"Читаю для инкремента: {SOURCE_CSV_PATH}")
        df_read = pd.read_csv(SOURCE_CSV_PATH)

        return df_read

    @task
    def filter_recent_data_for_increment(df: pd.DataFrame, logical_date_str_from_context: str) -> pd.DataFrame:

        hardcoded_logical_date_str = '2018-12-08' 
        print(f"Конкретная дата для теста: {hardcoded_logical_date_str} (вместо {{ds}}: {logical_date_str_from_context})")
        

        active_logical_date_str = hardcoded_logical_date_str
        
        if df.empty:
            print("DataFrame на входе в filter_recent_data_for_increment пустой. Фильтрация невозможна.")
            return df

        days_window = 3
        date_column_name = 'noted_date'
        
        try:
            df[date_column_name] = pd.to_datetime(df[date_column_name])
        except Exception as e:
            print(f"Ошибка при преобразовании колонки '{date_column_name}' в datetime: {e}")
            return pd.DataFrame() 
            
        current_logical_date = pd.to_datetime(active_logical_date_str) 
        start_filter_date = current_logical_date - timedelta(days=days_window - 1)
        end_filter_date = current_logical_date
        
        print(f"Логическая дата (current_logical_date): {current_logical_date.strftime('%Y-%m-%d')}")
        print(f"Начальная дата фильтра (start_filter_date): {start_filter_date.strftime('%Y-%m-%d')}")
        print(f"Конечная дата фильтра (end_filter_date): {end_filter_date.strftime('%Y-%m-%d')}")
        print(f"Фильтрую '{date_column_name}' с {start_filter_date.strftime('%Y-%m-%d')} по {end_filter_date.strftime('%Y-%m-%d')}")
        
        filtered_df = df[
            (df[date_column_name] >= start_filter_date) & 
            (df[date_column_name] <= end_filter_date)
        ].copy()
        
        print(f"Найдено строк за период: {len(filtered_df)}")
        return filtered_df

    @task
    def write_incremental_data_to_csv(df_recent: pd.DataFrame, logical_date_nodash_str_from_context: str):

        hardcoded_logical_date_for_filename = datetime.strptime('2018-12-08', '%Y-%m-%d') 
        active_logical_date_nodash_str = hardcoded_logical_date_for_filename.strftime('%Y%m%d')
        print(f"ИСПОЛЬЗУЕТСЯ ЗАХАРДКОЖЕННАЯ ДАТА ДЛЯ ИМЕНИ ФАЙЛА: {active_logical_date_nodash_str} (вместо {{ds_nodash}}: {logical_date_nodash_str_from_context})")

        if df_recent.empty:
            print(f"Нет данных для инкремента за {active_logical_date_nodash_str} для записи в файл.")
            return

        output_filename = f'transformed_weather_incremental_{active_logical_date_nodash_str}.csv'
        output_file_path = os.path.join(OUTPUT_DWH_DIR, output_filename)
        
        print(f"Пишу инкремент в: {output_file_path}")
        df_recent.to_csv(output_file_path, index=False)
        print(f"Инкрементальная выгрузка завершена. Сохранено строк: {len(df_recent)}")

    source_dataframe_inc = read_source_data_for_increment()

    recent_dataframe = filter_recent_data_for_increment(source_dataframe_inc, "{{ ds }}") 

    write_incremental_data_to_csv(recent_dataframe, "{{ ds_nodash }}")

full_load_dag_instance = full_load_transformed_weather_to_file_dag()
incremental_load_dag_instance = incremental_load_transformed_weather_to_file_dag()