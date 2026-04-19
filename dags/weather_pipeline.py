import logging
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
        result = subprocess.run(
            ['python', '/opt/airflow/extract/weather_extract.py'],
            capture_output=True, text=True, check=True
        )
        logger.info(result.stdout)
        logger.info("Extração concluída com sucesso!")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro na extração: {e.stderr}")
        raise

def transform_task():
    logger.info("Iniciando transformação de dados...")
    try:
        result = subprocess.run(
            ['python', '/opt/airflow/transform/weather_transform.py'],
            capture_output=True, text=True, check=True
        )
        logger.info(result.stdout)
        logger.info("Transformação concluída com sucesso!")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro na transformação: {e.stderr}")
        raise

def load_task():
    logger.info("Iniciando carga de dados...")
    try:
        result = subprocess.run(
            ['python', '/opt/airflow/load/weather_load.py'],
            capture_output=True, text=True, check=True
        )
        logger.info(result.stdout)
        logger.info("Carga concluída com sucesso!")
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro na carga: {e.stderr}")
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