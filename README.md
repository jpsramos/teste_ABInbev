# Teste ABInbev - Data Processing Project

[](https://github.com/jpsramos/teste_ABInbev/blob/main/README.md#descri%C3%A7%C3%A3o)


## **Descrição**


O projeto utiliza PySpark e Pandas para processar dados, gerenciar catálogos, ler arquivos CSV ou Parquet e salvar os resultados em três camadas de dados: Bronze, Silver e Gold. Inclui um ambiente Docker completo. 

Este repositório contém soluções para testes de avaliação, que envolvem consumir dados da API, transformá-los e armazená-los no data lake, seguindo a arquitetura Medallion (Níveis: Bronze para dados brutos, Silver para dados transformados e particionados e nível Gold para transformar e particionar dados).

### Tecnologias Utilizadas

- **Python** 3.8 ou superior
- **PySpark** 3.5.0
- **Pandas** 1.5.3
- **PyArrow** 12.0.1
- **API**: [Open Brewery DB](https://www.openbrewerydb.org/)
- **Orquestração**: [Airflow](https://airflow.apache.org/) (ou qualquer outra ferramenta de sua escolha)
- **Docker**: Para containerização do projeto

### Arquitetura do Data Lake (Medallion)

- **Bronze (Raw Data)**: Dados da API são consumidos e armazenados sem modificações.
- **Prata (Curated Data)**: Dados transformados e particionados por localização.
- **Ouro (Analytical Data)**: Dados agregados (quantidade de cervejarias por tipo e localização).

> Obs.: foi a feita a adição de de um diretorio anteedente Transient, para recepcionar o arquivo deu um forma como json ou csv. Bronze os converte para colunar com todos campos string. A idéia é simular um recepção de formato aleatório via integração qualquer


### Passos para Rodar o Projeto

- Clone o repositorio

ou

- Crie um do zero na sua interface ide, na raiz crie seu ambiente virtual siga a estrutura abaixo

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
│   ├── log/
│   |   └── YYYY-MM-DD/
│   │           └── *.csv
│   ├── silver/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
|   │           └── city=string/
│   │             └── *.parquet

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
├── tests/
│   ├── unit_test.py/
├── docker-compose.yml
├── Dockerfile
├── README.md
├── airflow.env
└── .dockerignore
```

### Arquitetura do Data Lake (Medallion)

- **Bronze (Raw Data)**: Dados da API são consumidos e armazenados sem modificações.
- **Prata (Curated Data)**: Dados transformados e particionados por localização.
- **Ouro (Analytical Data)**: Dados agregados (quantidade de cervejarias por tipo e localização).
