# 🌦️ Weather Data Pipeline

> Pipeline de dados completo para coleta, transformação e visualização de dados meteorológicos de cidades brasileiras.

<div align="center">
  <img width="600" height="400" alt="arquitetura" src="https://github.com/user-attachments/assets/a3e51a5d-9336-4c9a-9c50-3a27d65f0c11" />
</div>

---

## 📊 Dashboard

🔗 [Acessar Dashboard no Looker Studio](https://datastudio.google.com/reporting/e83512b5-ad06-4cbf-9df3-afa2f29a08e2)

---

## 🏗️ Arquitetura

O projeto foi construído com dois ambientes separados:

**Desenvolvimento** — extração, transformação e carga no PostgreSQL local via Docker.

**Produção** — extração, transformação e carga no BigQuery (GCP), orquestrado pelo Apache Airflow com agendamento diário.

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Extração | Python + Requests |
| Transformação | Python + Pandas |
| Orquestração | Apache Airflow |
| Banco Dev | PostgreSQL (Docker) |
| Banco Prod | BigQuery (GCP) |
| Infraestrutura | Docker + Docker Compose |
| Visualização | Looker Studio |
| Versionamento | Git + GitHub |

---

## ⚙️ Funcionalidades

- **Carga incremental** — o pipeline insere apenas dados novos, evitando reprocessamento desnecessário
- **Idempotência** — o pipeline pode rodar múltiplas vezes no mesmo dia sem duplicar dados
- **Retries automáticos** — em caso de falha, o Airflow tenta reexecutar a tarefa até 3 vezes automaticamente
- **Logs estruturados** — todas as etapas do pipeline registram logs detalhados para facilitar o monitoramento
- **Tratamento de erros** — cada etapa trata falhas de conexão, timeout e dados ausentes de forma controlada
- **Dois ambientes** — configurações separadas para desenvolvimento e produção via variáveis de ambiente

---

## 🌍 Cidades Monitoradas

- 🏙️ São Paulo
- 🌊 Rio de Janeiro
- 🌞 Salvador
- 🏛️ Brasília
- 🏡 Uberlândia

---

## 📁 Estrutura do Projeto

```
weather-data-pipeline/
│
├── dags/                        # DAGs do Airflow
│   └── weather_pipeline.py      # Pipeline principal
│
├── extract/                     # Scripts de extração
│   └── weather_extract.py       # Extração da API OpenWeatherMap
│
├── transform/                   # Scripts de transformação
│   └── weather_transform.py     # Limpeza e padronização dos dados
│
├── load/                        # Scripts de carga
│   └── weather_load.py          # Carga no PostgreSQL e BigQuery
│
├── data/
│   ├── raw/                     # Dados brutos da API (JSON)
│   └── processed/               # Dados transformados (JSON)
│
├── docker/                      # Configurações Docker
├── logs/                        # Logs do Airflow
├── tests/                       # Testes do projeto
├── .env.dev                     # Variáveis de ambiente - desenvolvimento
├── .env.prod                    # Variáveis de ambiente - produção
├── docker-compose.yml           # Orquestração dos containers
└── requirements.txt             # Dependências Python
```

---

## 🔄 Fluxo do Pipeline

```
OpenWeatherMap API
        ↓
extract_weather_data (Python)
        ↓
transform_weather_data (Python)
        ↓
load_weather_data
        ↓
PostgreSQL (dev) / BigQuery (prod)
        ↓
Looker Studio Dashboard
```

O Airflow agenda e orquestra todo esse fluxo diariamente às 00:00 UTC.

---

## 🚀 Como Rodar o Projeto

### Pré-requisitos

- Docker Desktop
- Python 3.14+
- Conta no GCP com BigQuery habilitado
- API Key do OpenWeatherMap

### 1. Clone o repositório

```bash
git clone https://github.com/carolczeder/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2. Configure as variáveis de ambiente

```bash
# .env.dev
OPENWEATHER_API_KEY=sua_api_key
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# .env.prod
ENV=prod
OPENWEATHER_API_KEY=sua_api_key
BQ_PROJECT=seu_projeto_gcp
BQ_DATASET=weather_dataset
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Suba os containers

```bash
docker compose up -d
```

### 5. Acesse o Airflow

```
http://localhost:8080
Usuário: admin
Senha: admin
```

### 6. Ative a DAG `weather_pipeline`

---

## 📈 Dados Coletados

| Campo | Descrição |
|---|---|
| cidade | Nome da cidade |
| pais | Código do país |
| temperatura | Temperatura atual (°C) |
| sensacao_termica | Sensação térmica (°C) |
| temperatura_min | Temperatura mínima (°C) |
| temperatura_max | Temperatura máxima (°C) |
| umidade | Umidade relativa (%) |
| pressao | Pressão atmosférica (hPa) |
| velocidade_vento | Velocidade do vento (m/s) |
| descricao | Descrição do clima |
| data_hora | Data e hora da coleta |

---

## 👩‍💻 Autora

**Carol Czeder**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-carolineczeder-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/carolineczeder/)
[![GitHub](https://img.shields.io/badge/GitHub-carolczeder-181717?style=flat&logo=github&logoColor=white)](https://github.com/carolczeder)
