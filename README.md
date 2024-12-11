# Teste ABInbev - Data Processing Project

## **Descrição**
Este projeto realiza o processamento de dados utilizando **PySpark** e **Pandas**. Ele é projetado para gerenciar diretórios, ler arquivos CSV ou Parquet e salvar os resultados processados em diferentes camadas de dados: **bronze**, **silver** e **gold**.

## **Requisitos**
Certifique-se de que os seguintes requisitos estão instalados antes de executar o projeto:
- **Python** 3.8 ou superior
- **PySpark** 3.5.0
- **Pandas** 1.5.3
- **PyArrow** 12.0.1


## **Requisitos**
Certifique-se de que os seguintes requisitos estão instalados antes de executar o projeto:
- **Python** 3.8 ou superior
- **PySpark** 3.5.0
- **Pandas** 1.5.3
- **PyArrow** 12.0.1


## **Requisitos**
Certifique-se de que os seguintes requisitos estão instalados antes de executar o projeto:
- **Python** 3.8 ou superior
- **PySpark** 3.5.0
- **Pandas** 1.5.3
- **PyArrow** 12.0.1

## **Estrutura do Projeto**
```plaintext
Projeto ABInbev
├── .venv
├── data/
│   ├── transient/
│   │   └── Breweries/
│   │       └── YYYY-MM-DD/
│   │           └── *.csv
│   ├── bronze/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
│   │           └── *.parquet
│   ├── silver/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
│   │           └── *.parquet
│   └── gold/
│       └── Breweries/
│           └── dat_process=YYYY-MM-DD/
│               └── *.parquet
├── jobs/
│   ├── scripts/
│   │   └── ingestion.py
├── utils/
│   ├── api/
│   │   └── restfull.py
├── config/
│   ├── config.py
├── docker-compose.yml
├── Dockerfile
├── README.md
├── airflow.env
└── .dockerignore
