import datetime as dt
from dateutil.relativedelta import relativedelta

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
        start_date=dt.datetime(2023, 9, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        tasks = []

        for offset in range(0, 1):
            tasks.append(
                PythonOperator(
                    task_id=f'get_requests_offset_{offset}',
                    python_callable=etl,
                    op_kwargs={
                        'offset': offset,
                        'data_type': 'stage_crm_requests',
                    },
                )
            )

        tasks

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        pass

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

    with TaskGroup('Проверки') as data_checks:

        pass

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
