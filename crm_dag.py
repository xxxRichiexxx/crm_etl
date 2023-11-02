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
        'crm_dag',
        default_args=default_args,
        description='Получение данных из CRM.',
        start_date=dt.datetime(2022, 1, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        t2 = PythonOperator(
            task_id=f'get_requests_offset_2',
            python_callable=etl,
            op_kwargs={
                'offset': 2,
                'data_type': 'stage_crm_requests',
            },
        )

        t1 = PythonOperator(
            task_id=f'get_requests_offset_1',
            python_callable=etl,
            op_kwargs={
                'offset': 1,
                'data_type': 'stage_crm_requests',
            },
        )

        t0 = PythonOperator(
            task_id=f'get_requests_offset_0',
            python_callable=etl,
            op_kwargs={
                'offset': 0,
                'data_type': 'stage_crm_requests',
            },
        )

        t2 >> t1 >> t0

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        pass

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        dm_crm_slow_requests = VerticaOperator(
                    task_id='dm_crm_requests',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_crm_requests.sql',
                )

    with TaskGroup('Проверки') as data_checks:

        dm_crm_slow_requests_check = VerticaOperator(
                    task_id='dm_crm_requests_check',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_crm_requests_check.sql',
                    params={
                        'dm': 'dm_crm_requests',
                    }
                )

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
