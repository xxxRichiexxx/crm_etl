import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator

from crm_etl.scripts.collable import etl


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'crm_dag_sales',
        default_args=default_args,
        description='Получение данных из CRM. Продажи(Выдачи)',
        start_date=dt.datetime(2022, 1, 1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        t2 = PythonOperator(
            task_id=f'get_sales_offset_2',
            python_callable=etl,
            op_kwargs={
                'offset': 2,
                'table_name': 'stage_crm_sales',
                'datatype': 'sales',
            },
        )

        t1 = PythonOperator(
            task_id=f'get_sales_offset_1',
            python_callable=etl,
            op_kwargs={
                'offset': 1,
                'table_name': 'stage_crm_sales',
                'datatype': 'sales',
            },
        )

        t0 = PythonOperator(
            task_id=f'get_sales_offset_0',
            python_callable=etl,
            op_kwargs={
                'offset': 0,
                'table_name': 'stage_crm_sales',
                'datatype': 'sales',
            },
        )

        [t2, t1, t0]

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        pass

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        dm_crm_sales = VerticaOperator(
            task_id='dm_crm_sales',
            vertica_conn_id='vertica',
            sql='scripts/dm_crm_sales.sql',
        )
        dm_crm_sales

    with TaskGroup('Проверки') as data_checks:

        dm_crm_sales_check = VerticaOperator(
            task_id='dm_crm_sales_check',
            vertica_conn_id='vertica',
            sql='scripts/dm_crm_sales_check.sql',
                params={
                    'dm': 'dm_crm_sales',
                }
        )

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
