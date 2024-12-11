# Teste ABInbev - Data Processing Project

[](https://github.com/jpsramos/teste_ABInbev/blob/main/README.md#descri%C3%A7%C3%A3o)


## **Descrição**


O projeto utiliza PySpark e Pandas para processar dados, gerenciar catálogos, ler arquivos CSV ou Parquet e salvar os resultados em três camadas de dados: Bronze, Silver e Gold. Inclui um ambiente Docker completo. 

Este repositório contém soluções para testes de avaliação, que envolvem consumir dados da API, transformá-los e armazená-los no data lake, seguindo a arquitetura Medallion (Níveis: Bronze para dados brutos, Silver para dados transformados e particionados e nível Gold para transformar e particionar dados).

### Tecnologias Utilizadas

- **Python** 3.9
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
├── dags/
│   ├── dag_ingestion.py
├── data/
│   ├── transient/
│   │   └── Breweries/
│   │       └── YYYY-MM-DD/
│   │           └── *.csv
│   ├── bronze/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
│   │           └── *.parquet
│   └── gold/
│   |   └── Breweries/
│   |       └── dat_process=YYYY-MM-DD/
│   |            └── *.parquet
│   ├── log/
│   |   └── YYYY-MM-DD/
│   │           └── *.csv
│   ├── silver/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
|   │           └── city=string/
│   │             └── *.parquet
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

### Criação do ambiente

- **1**: Após a criação da estrutura acima, edite os arquivos docker-compose.yml, Dockerfile, airflow.env .dockerignore exatamente como estão os mesmos arquivos nesse repositório. Claro, isso vale para todos os demais arquivos
- **2**: Docker instalado. Execute o seguinte comando na raiz onde encontrase o arquivo docker-compose.yml
```
docker-compose up -d --build
```
Este comando criará suma imagem e docker, utilizará o docker-compose.yml como template para baixar suma imagem pyspark e o Dockerfile para instalr todos os recursos necesários. Os pacotes inserido no Dockerfile garante que não haverá falha na compatibilidade das aplicaçãoes, fique atento em propor a utilização de algum quanto suas dependências.

- **3**: Após finalização ao executar o comando docker-compose -d , poderá confirmar seus containers ativos, será listado os seguintes:
```
spark-master
spark-worker
airflow-scheduler
airflow-webserver
```
- **4**: Abra seu docker e deu um start no airflow-webserver, a configuração do docker-compose.yml pemitirá que visualize o airflow ativo via navegador através do seguinte endereço, http://localhost:8080.

- **5**: Não automatizei sua criação, será necessário acessar Admin > Connections e criar conenexão uma para o fluxo funcionar:
```
Connection Id *	: spark-conn
Connection Type *: spark
Host: spark://spark-master
Port: 7077
```

