import json
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(".env.dev")


def conectar_banco():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_db",
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )


def inserir_dados(conn, dados):
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
    print(f"Inserido: {dados['cidade']}")


def carregar_dados():
    pasta_processed = "data/processed"
    arquivos = [f for f in os.listdir(pasta_processed) if f.endswith(".json")]

    print(f"Carregando {len(arquivos)} arquivos no PostgreSQL...")

    conn = conectar_banco()

    for arquivo in arquivos:
        caminho = os.path.join(pasta_processed, arquivo)

        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)

        inserir_dados(conn, dados)

    conn.close()
    print("Carga concluída!")


if __name__ == "__main__":
    carregar_dados()