import json
import os
from datetime import datetime


def transformar_dados(dados_brutos):
    return {
        "cidade": dados_brutos["name"],
        "pais": dados_brutos["sys"]["country"],
        "temperatura": dados_brutos["main"]["temp"],
        "sensacao_termica": dados_brutos["main"]["feels_like"],
        "temperatura_min": dados_brutos["main"]["temp_min"],
        "temperatura_max": dados_brutos["main"]["temp_max"],
        "umidade": dados_brutos["main"]["humidity"],
        "pressao": dados_brutos["main"]["pressure"],
        "velocidade_vento": dados_brutos["wind"]["speed"],
        "descricao": dados_brutos["weather"][0]["description"],
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def processar_arquivos():
    pasta_raw = "data/raw"
    pasta_processed = "data/processed"

    arquivos = [f for f in os.listdir(pasta_raw) if f.endswith(".json")]

    print(f"Encontrados {len(arquivos)} arquivos para processar...")

    for arquivo in arquivos:
        caminho = os.path.join(pasta_raw, arquivo)

        with open(caminho, "r", encoding="utf-8") as f:
            dados_brutos = json.load(f)

        dados_transformados = transformar_dados(dados_brutos)

        nome_saida = arquivo.replace(".json", "_processed.json")
        caminho_saida = os.path.join(pasta_processed, nome_saida)

        with open(caminho_saida, "w", encoding="utf-8") as f:
            json.dump(dados_transformados, f, ensure_ascii=False, indent=4)

        print(f"Processado: {nome_saida}")

    print("Transformação concluída!")


if __name__ == "__main__":
    processar_arquivos()