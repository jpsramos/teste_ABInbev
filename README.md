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

Dockerfile
```
FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        procps \
        openjdk-11-jdk \
        vim && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

RUN mkdir -p /tmp/spark-temp && chmod -R 777 /tmp/spark-temp

USER airflow

RUN pip install apache-airflow==2.7.2 apache-airflow-providers-apache-spark==4.0.1 pyspark==3.5.0 \
    openlineage-airflow==1.24.1 streamlit==1.26.0 requests==2.31.0 duckdb==0.9.1 pyarrow==14.0.0
```
docker-compose.ymml
```
version: '4'

x-spark-common: &spark-common
  image: bitnami/spark:3.5.0-debian-11-r0
  volumes:
    - ./jobs:/opt/bitnami/spark/jobs
    - ./data:/opt/airflow/data
  networks:
    - code-with-yu

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  env_file:
    - airflow.env
  volumes:
    - ./jobs:/opt/airflow/jobs
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./data:/opt/airflow/data
  depends_on:
    - postgres
  networks:
    - code-with-yu

services:
  spark-master:
    <<: *spark-common
    container_name: spark-master
    command: bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      PYSPARK_PYTHON: /usr/bin/python3.9
      PYSPARK_DRIVER_PYTHON: /usr/bin/python3.9
    ports:
      - "9090:8080"
      - "7077:7077"

  spark-worker:
    <<: *spark-common
    container_name: spark-worker
    command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_WORKER_CORES: 2
      SPARK_WORKER_MEMORY: 1g
      SPARK_MASTER_URL: spark://spark-master:7077
      PYSPARK_PYTHON: /usr/bin/python3.9
      PYSPARK_DRIVER_PYTHON: /usr/bin/python3.9

  postgres:
    image: postgres:14.0
    container_name: airflow-postgres
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    networks:
      - code-with-yu

  webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - scheduler

  scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: bash -c "airflow db migrate && airflow users create --username admin --firstname Jonnathans --lastname Silva --role Admin --email airscholar@gmail.com --password admin && airflow scheduler"

networks:
  code-with-yu:
```

- **2**: Docker instalado. Execute o seguinte comando na raiz onde encontra-se o arquivo docker-compose.yml
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

![image](https://github.com/user-attachments/assets/7a2bd88f-abd6-4315-8769-24d1da1fb852)

![image](https://github.com/user-attachments/assets/49a265d3-ba07-4efe-b918-5ac864e109e5)

- **5**: Não automatizei sua criação, será necessário acessar Admin > Connections e criar conenexão uma para o fluxo funcionar:
```
Connection Id *	: spark-conn
Connection Type *: spark
Host: spark://spark-master
Port: 7077
```
![image](https://github.com/user-attachments/assets/ec689a44-de36-4b0c-8aad-d4079ad447a4)

<img width="944" alt="image" src="https://github.com/user-attachments/assets/b506f3bf-1c2c-42de-85f5-4692cf5eb6d3">

- **6**: Dag disponível

![image](https://github.com/user-attachments/assets/cf9cfa33-1052-403b-b483-39b35ee0946e)

- **7**: Processo iniciado

![image](https://github.com/user-attachments/assets/bb2efec6-6bd7-4919-9671-3a01daad758a)

- **8**: Processo finalizado
![image](https://github.com/user-attachments/assets/2177109b-2979-4df2-b57e-7ad4fd1ec564)


### Monitoramento

No script de inserção foi criado um arquivo com a seguinte estrutura para uma analise do processo. A ideia é ter o principio de uma fonte de registro a qual pode ser amadurecida com toda certeza.

```
Directory,File Name,File Path,Start Time,End Time,Record Count
/opt/airflow/data/transient/Breweries/2024-12-11,20241211_0006.csv,/opt/airflow/data/transient/Breweries/2024-12-11/20241211_0006.csv,2024-12-11 00:06:21,2024-12-11 00:11:52,8355

```

### Tratamentos
Ttrasformação para Silver seguiu boas práticas de nomenclatura de campos, para familiaridade de quem os conhece por sua origem. A evolução desse retona todos com brewary na frente de seus nomes.

```
column_mapping = {
    "id": "brewery_id",
    "name": "brewery_name",
    "brewery_type": "type",
    "address_1": "primary_address",
    "address_2": "secondary_address",
    "address_3": "tertiary_address",
    "city": "city",
    "state_province": "state",
    "postal_code": "zip_code",
    "country": "country",
    "longitude": "longitude",
    "latitude": "latitude",
    "phone": "phone_number",
    "website_url": "website",
    "state": "state_code",
    "street": "street_name"
}
```

