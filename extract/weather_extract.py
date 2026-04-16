import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(".env.dev")

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
    params = {
        "q": cidade,
        "appid": API_KEY,
        "units": "metric",
        "lang": "pt_br"
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar {cidade}: {response.status_code}")
        return None


def salvar_dados(dados, cidade):
    agora = datetime.now().strftime("%Y%m%d_%H%M%S")
    cidade_formatada = cidade.split(",")[0].replace(" ", "_").lower()
    nome_arquivo = f"data/raw/{cidade_formatada}_{agora}.json"

    with open(nome_arquivo, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)

    print(f"Dados salvos: {nome_arquivo}")


def main():
    print(f"Iniciando extração: {datetime.now()}")

    for cidade in CIDADES:
        print(f"Extraindo dados de {cidade}...")
        dados = extrair_clima(cidade)

        if dados:
            salvar_dados(dados, cidade)

    print(f"Extração concluída: {datetime.now()}")


if __name__ == "__main__":
    main()