import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from extract.weather_extract import main as extract_main
from transform.weather_transform import processar_arquivos as transform_main
from load.weather_load import carregar_dados as load_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'carol',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def extract_task():
    logger.info("Iniciando extração de dados...")
    try:
        extract_main()
        logger.info("Extração concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro na extração: {e}")
        raise

def transform_task():
    logger.info("Iniciando transformação de dados...")
    try:
        transform_main()
        logger.info("Transformação concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro na transformação: {e}")
        raise

def load_task():
    logger.info("Iniciando carga de dados...")
    try:
        load_main()
        logger.info("Carga concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro na carga: {e}")
        raise

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Pipeline de dados de clima - OpenWeatherMap',
    schedule_interval='@daily',
    catchup=False,
    tags=['weather', 'pipeline', 'dados']
) as dag:

    extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_task,
    )

    extract >> transform >> load