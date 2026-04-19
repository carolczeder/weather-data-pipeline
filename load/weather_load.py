import json
import os
import logging
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery

ENV = os.getenv("ENV", "dev")

if ENV == "prod":
    load_dotenv(".env.prod")
else:
    load_dotenv(".env.dev")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conectar_postgres():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_db",
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )


def conectar_bigquery():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "docker/gcp-credentials.json"
    return bigquery.Client()


def ja_existe_postgres(conn, cidade, data_hora):
    cursor = conn.cursor()
    sql = """
        SELECT COUNT(*) FROM clima
        WHERE cidade = %s
        AND DATE(data_hora) = DATE(%s)
    """
    cursor.execute(sql, (cidade, data_hora))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0


def ja_existe_bigquery(client, cidade, data_hora):
    query = f"""
        SELECT COUNT(*) as total FROM `{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.clima`
        WHERE cidade = '{cidade}'
        AND DATE(data_hora) = DATE('{data_hora}')
    """
    result = client.query(query).result()
    for row in result:
        return row.total > 0
    return False


def inserir_postgres(conn, dados):
    if ja_existe_postgres(conn, dados["cidade"], dados["data_hora"]):
        logger.info(f"Dado já existe para {dados['cidade']} hoje — pulando!")
        return

    cursor = conn.cursor()
    sql = """
        INSERT INTO clima (
            cidade, pais, temperatura, sensacao_termica,
            temperatura_min, temperatura_max, umidade,
            pressao, velocidade_vento, descricao, data_hora
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (
        dados["cidade"], dados["pais"], dados["temperatura"],
        dados["sensacao_termica"], dados["temperatura_min"],
        dados["temperatura_max"], dados["umidade"], dados["pressao"],
        dados["velocidade_vento"], dados["descricao"], dados["data_hora"]
    ))
    conn.commit()
    cursor.close()
    logger.info(f"Inserido no PostgreSQL: {dados['cidade']}")


def inserir_bigquery(client, dados):
    if ja_existe_bigquery(client, dados["cidade"], dados["data_hora"]):
        logger.info(f"Dado já existe para {dados['cidade']} hoje no BQ — pulando!")
        return

    table_id = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.clima"

    df = pd.DataFrame([{
        "cidade": dados["cidade"],
        "pais": dados["pais"],
        "temperatura": float(dados["temperatura"]),
        "sensacao_termica": float(dados["sensacao_termica"]),
        "temperatura_min": float(dados["temperatura_min"]),
        "temperatura_max": float(dados["temperatura_max"]),
        "umidade": int(dados["umidade"]),
        "pressao": int(dados["pressao"]),
        "velocidade_vento": float(dados["velocidade_vento"]),
        "descricao": dados["descricao"],
        "data_hora": pd.Timestamp(dados["data_hora"])
    }])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info(f"Inserido no BigQuery: {dados['cidade']}")


def carregar_dados():
    pasta_processed = "data/processed"
    arquivos = [f for f in os.listdir(pasta_processed) if f.endswith(".json")]

    logger.info(f"Ambiente: {ENV}")
    logger.info(f"Carregando {len(arquivos)} arquivos...")

    try:
        if ENV == "prod":
            client = conectar_bigquery()
        else:
            conn = conectar_postgres()

        for arquivo in arquivos:
            caminho = os.path.join(pasta_processed, arquivo)
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)

            if ENV == "prod":
                inserir_bigquery(client, dados)
            else:
                inserir_postgres(conn, dados)

        if ENV != "prod":
            conn.close()

        logger.info("Carga concluída!")

    except Exception as e:
        logger.error(f"Erro na carga: {e}")
        raise


if __name__ == "__main__":
    carregar_dados()