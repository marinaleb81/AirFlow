from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Определяем имя соединения для MySQL, которое мы создали в Airflow UI
MYSQL_CONN_ID = "mysql_conn"
# Определяем имя соединения для PostgreSQL, которое мы создали в Airflow UI
POSTGRES_CONN_ID = "pg_conn"
# Имя таблицы, которую мы синхронизируем
TABLE_NAME = "customers"


def extract_from_mysql_and_load_to_postgres():
    """
    Извлекает все данные из таблицы MySQL и загружает их в PostgreSQL,
    используя стратегию "UPSERT" (INSERT ON CONFLICT DO UPDATE).
    """
    # Получаем хук для MySQL
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    # Получаем хук для PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. Извлечение данных из MySQL
    # Соединение уже установлено хуком, используем его для выполнения запроса
    # mysql_conn = mysql_hook.get_conn() # Сам хук управляет соединением
    # mysql_cursor = mysql_conn.cursor(dictionary=True) # dictionary=True чтобы получать строки как словари
    
    # Для MySqlHook.run() или .get_records() не нужен явный курсор, если просто получаем данные
    print(f"Извлечение данных из MySQL таблицы '{TABLE_NAME}'...")
    # Используем get_records, который возвращает список кортежей
    records = mysql_hook.get_records(sql=f"SELECT id, full_name, email, country, created_at FROM {TABLE_NAME}")
    
    if not records:
        print("Нет данных для извлечения из MySQL. Завершение.")
        return

    print(f"Извлечено {len(records)} записей из MySQL.")

    # 2. Загрузка данных в PostgreSQL
    # Мы будем использовать INSERT ... ON CONFLICT DO UPDATE для "upsert"
    # Это гарантирует, что если запись с таким id уже существует, она будет обновлена,
    # а если нет - будет вставлена новая.
    
    # PostgreSQL ожидает %s для плейсхолдеров в запросе
    upsert_sql = f"""
    INSERT INTO {TABLE_NAME} (id, full_name, email, country, created_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        full_name = EXCLUDED.full_name,
        email = EXCLUDED.email,
        country = EXCLUDED.country,
        created_at = EXCLUDED.created_at;
    """
    
    print(f"Загрузка данных в PostgreSQL таблицу '{TABLE_NAME}'...")
    # pg_hook.run() может принимать список кортежей для bulk-операций,
    # но для ON CONFLICT лучше выполнять построчно или убедиться, что run поддерживает это для batch.
    # Для большей ясности и контроля над каждой операцией upsert, можно сделать цикл:
    successful_upserts = 0
    failed_upserts = []

    for record in records:
        try:
            pg_hook.run(upsert_sql, parameters=record)
            successful_upserts += 1
        except Exception as e:
            failed_upserts.append(record)
            print(f"Ошибка при загрузке записи {record}: {e}")

    print(f"Успешно загружено/обновлено {successful_upserts} записей в PostgreSQL.")
    if failed_upserts:
        print(f"Не удалось загрузить {len(failed_upserts)} записей:")
        for failed_record in failed_upserts:
            print(failed_record)


with DAG(
    dag_id="mysql_to_postgres_replication",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"), # Укажи актуальную дату начала
    schedule="@hourly", # Запускать каждый час (или выбери другое расписание)
    catchup=False,
    tags=["data_replication", "mysql", "postgres"],
    doc_md="""\
        ### DAG для репликации данных из MySQL в PostgreSQL

        Этот DAG извлекает все данные из таблицы `customers` в MySQL
        и загружает их в аналогичную таблицу в PostgreSQL.
        Используется стратегия UPSERT (INSERT ON CONFLICT DO UPDATE)
        для синхронизации записей по полю `id`.
    """,
) as dag:
    replicate_mysql_to_postgres_task = PythonOperator(
        task_id="replicate_mysql_to_postgres",
        python_callable=extract_from_mysql_and_load_to_postgres,
    )