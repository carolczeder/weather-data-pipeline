import requests
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(".env.dev")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENWEATHER_API_KEY", "sua_api_key_aqui")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

CIDADES = [
    "Sao Paulo,BR",
    "Rio de Janeiro,BR",
    "Uberlandia,BR",
    "Salvador,BR",
    "Brasilia,BR"
]


def extrair_clima(cidade):
    try:
        params = {
            "q": cidade,
            "appid": API_KEY,
            "units": "metric",
            "lang": "pt_br"
        }

        response = requests.get(BASE_URL, params=params, timeout=10)

        if response.status_code == 200:
            logger.info(f"Dados extraídos com sucesso: {cidade}")
            return response.json()
        else:
            logger.error(f"Erro ao buscar {cidade}: status {response.status_code}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao buscar {cidade}")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"Erro de conexão ao buscar {cidade}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar {cidade}: {e}")
        return None


def salvar_dados(dados, cidade):
    try:
        agora = datetime.now().strftime("%Y%m%d_%H%M%S")
        cidade_formatada = cidade.split(",")[0].replace(" ", "_").lower()
        nome_arquivo = f"data/raw/{cidade_formatada}_{agora}.json"

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)

        logger.info(f"Dados salvos: {nome_arquivo}")

    except Exception as e:
        logger.error(f"Erro ao salvar dados de {cidade}: {e}")
        raise


def main():
    logger.info(f"Iniciando extração: {datetime.now()}")

    for cidade in CIDADES:
        logger.info(f"Extraindo dados de {cidade}...")
        dados = extrair_clima(cidade)

        if dados:
            salvar_dados(dados, cidade)
        else:
            logger.warning(f"Sem dados para {cidade} — pulando!")

    logger.info(f"Extração concluída: {datetime.now()}")


if __name__ == "__main__":
    main()