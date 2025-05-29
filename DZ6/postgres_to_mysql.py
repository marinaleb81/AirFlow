from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Имена соединений, созданных в Airflow UI
POSTGRES_CONN_ID = "pg_conn"
MYSQL_CONN_ID = "mysql_conn"
TABLE_NAME = "customers"


def extract_from_postgres_and_load_to_mysql():
    """
    Извлекает все данные из таблицы PostgreSQL и загружает их в MySQL,
    используя стратегию "UPSERT" (INSERT ... ON DUPLICATE KEY UPDATE ...).
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    # 1. Извлечение данных из PostgreSQL
    print(f"Извлечение данных из PostgreSQL таблицы '{TABLE_NAME}'...")
    # get_records возвращает список кортежей
    records = pg_hook.get_records(sql=f"SELECT id, full_name, email, country, created_at FROM {TABLE_NAME}")
    
    if not records:
        print("Нет данных для извлечения из PostgreSQL. Завершение.")
        return

    print(f"Извлечено {len(records)} записей из PostgreSQL.")

    # 2. Загрузка данных в MySQL
    # MySQL использует INSERT ... ON DUPLICATE KEY UPDATE для "upsert"
    # Плейсхолдеры в MySQL - %s
    
    upsert_sql = f"""
    INSERT INTO {TABLE_NAME} (id, full_name, email, country, created_at)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        full_name = VALUES(full_name),
        email = VALUES(email),
        country = VALUES(country),
        created_at = VALUES(created_at);
    """
    
    print(f"Загрузка данных в MySQL таблицу '{TABLE_NAME}'...")
    successful_upserts = 0
    failed_upserts = []

    for record in records:
        try:
            # MySqlHook.run ожидает кортеж/список параметров
            mysql_hook.run(upsert_sql, parameters=record)
            successful_upserts += 1
        except Exception as e:
            failed_upserts.append(record)
            print(f"Ошибка при загрузке записи {record}: {e}")
            
    print(f"Успешно загружено/обновлено {successful_upserts} записей в MySQL.")
    if failed_upserts:
        print(f"Не удалось загрузить {len(failed_upserts)} записей:")
        for failed_record in failed_upserts:
            print(failed_record)


with DAG(
    dag_id="postgres_to_mysql_replication",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"), # Укажи актуальную дату начала
    schedule="@hourly", # Запускать каждый час (или другое расписание)
    catchup=False,
    tags=["data_replication", "postgres", "mysql"],
    doc_md="""\
        ### DAG для репликации данных из PostgreSQL в MySQL

        Этот DAG извлекает все данные из таблицы `customers` в PostgreSQL
        и загружает их в аналогичную таблицу в MySQL.
        Используется стратегия UPSERT (INSERT ... ON DUPLICATE KEY UPDATE ...)
        для синхронизации записей по полю `id`.
    """,
) as dag:
    replicate_postgres_to_mysql_task = PythonOperator(
        task_id="replicate_postgres_to_mysql",
        python_callable=extract_from_postgres_and_load_to_mysql,
    )