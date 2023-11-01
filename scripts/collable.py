import pandas as pd
import glob
import os
import sys
import sqlalchemy as sa
from urllib.parse import quote
from airflow.hooks.base import BaseHook

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CODE_DIR_PATH = os.path.join(BASE_DIR, 'lib')
sys.path.append(CODE_DIR_PATH)

from crm import CRMExtractor


def extract(source_url, source_username, source_password, execution_date, path):
    """Извлечение данных из источника."""

    print('ИЗВЛЕЧЕНИЕ ДАННЫХ')

    extr = CRMExtractor(
        source_username, 
        source_password,
        source_url,
        path,
        execution_date,
        execution_date,
    )

    extr.get_requests()

    result = pd.read_excel(
        fr'{extr.file}',
        sheet_name='Отчет',
        header=None,
        skiprows=11,
        dtype=str,
    )

    # очищаем целевую папку
    file_pattern = os.path.join(path, extr.file_pattern)
    matching_files = glob.glob(file_pattern)
    if matching_files:
        for file_path in matching_files:
            os.remove(file_path)
            print(f"Датафрейм создан, файл экселя удален: {file_path}")
    else:
        print(f"Файлы, соответствующие шаблону имени 'Obracsheniya*', не найдены для загрузки.")

    return result


def transform(data, execution_date, data_type):
    """Преобразование/трансформация данных."""

    print('ТРАНСФОРМАЦИЯ ДАННЫХ')
    print(data)
    
    if data_type == 'stage_crm_requests':
        data.columns = [
            "Skorost",
            "Region",
            'Holding',
            'NomerObrashenia',
            'KodDC',
            'Dealer',
            'DataSozdania',
            'DataSmeniStatusa',
            'Status',
            'Comments',
            'Prichina',
            'IstochnicTrafica',
            'TipZaiavki',
            'Domen',
            'IPAdresClienta',
            'NomerRL',
            'ModelInteresuiushegoTS',
            'ClientID',
            'Client',
            'Phone',
            'Email',
            'TextObrashenia',
            'SoglasieNaObrabotcuPD',
            'SoglasieNaPoluchenieInformacii',
            'ID',
            'ObrashenieZakrito',
            'TegiClienta',
            'StatusRL',
            'EtapProdaz',
            'OblastClienta',
            'ObsheeVremiaZvonca',
            'StatusSviazannogoSobitiya',
            'TypeOcherednogoSobitiya',
            'DataOcherednogoSobitiya',
            'OtvetstvenniyZaRL',
        ]

    data['period'] = execution_date
    return data


def load(dwh_engine, data, data_type, execution_date):
    """Загрузка данных в хранилище."""

    print('ЗАГРУЗКА ДАННЫХ')
    if not data.empty:

        print(data)

        command = f"""
            SELECT DROP_PARTITIONS(
                'sttgaz.{data_type}',
                '{execution_date.replace(day=1)}',
                '{execution_date.replace(day=1)}'
            );
        """
        print(command)

        dwh_engine.execute(command)

        data.to_sql(
            f'{data_type}',
            dwh_engine,
            schema='sttgaz',
            if_exists='append',
            index=False,
        )
    else:
        print('Нет новых данных для загрузки.')


def etl(data_type, offset=None, **context):
    """Запускаем ETL-процесс для заданного типа данных."""

    source_con = BaseHook.get_connection('crm')
    source_username = source_con.login
    source_password = quote(source_con.password)
    source_url = source_con.host

    dwh_con = BaseHook.get_connection('vertica')
    ps = quote(dwh_con.password)
    dwh_engine = sa.create_engine(
        f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
    )

    if offset:
        month = context['execution_date'].month - offset
        if month <= 0:
            month = 12 + month
            execution_date = context['execution_date'].date().replace(month = month, year = context['execution_date'].year - 1, day=1)
        else:
            execution_date = context['execution_date'].date().replace(month = month, day=1)
    else:
        execution_date = context['execution_date'].date().replace(day=1)

    path = f"/tmp/{execution_date}"

    data = extract(source_url, source_username, source_password, execution_date, path)
    data = transform(data, execution_date, data_type)
    load(dwh_engine, data, data_type, execution_date)
