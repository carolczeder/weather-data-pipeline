import json
import os
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(".env.dev")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conectar_banco():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_db",
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )


def ja_existe(conn, cidade, data_hora):
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


def inserir_dados(conn, dados):
    if ja_existe(conn, dados["cidade"], dados["data_hora"]):
        logger.info(f"Dado já existe para {dados['cidade']} hoje — pulando!")
        return

    cursor = conn.cursor()
    sql = """
        INSERT INTO clima (
            cidade, pais, temperatura, sensacao_termica,
            temperatura_min, temperatura_max, umidade,
            pressao, velocidade_vento, descricao, data_hora
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    cursor.execute(sql, (
        dados["cidade"],
        dados["pais"],
        dados["temperatura"],
        dados["sensacao_termica"],
        dados["temperatura_min"],
        dados["temperatura_max"],
        dados["umidade"],
        dados["pressao"],
        dados["velocidade_vento"],
        dados["descricao"],
        dados["data_hora"]
    ))
    conn.commit()
    cursor.close()
    logger.info(f"Inserido: {dados['cidade']}")


def carregar_dados():
    pasta_processed = "data/processed"
    arquivos = [f for f in os.listdir(pasta_processed) if f.endswith(".json")]

    logger.info(f"Carregando {len(arquivos)} arquivos no PostgreSQL...")

    try:
        conn = conectar_banco()

        for arquivo in arquivos:
            caminho = os.path.join(pasta_processed, arquivo)

            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)

            inserir_dados(conn, dados)

        conn.close()
        logger.info("Carga concluída!")

    except Exception as e:
        logger.error(f"Erro na carga: {e}")
        raise


if __name__ == "__main__":
    carregar_dados()