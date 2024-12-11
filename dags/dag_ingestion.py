import sys
import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Adicione o diretório base do projeto ao sys.path
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_path)

# Agora você pode importar a classe corretamente
from jobs.utils.api.restfull import DataProcessor as restfull

# Função Python que será executada no novo step
def run_restfull_api():
    print("Running custom Python script...")
    # Instancia a classe DataProcessor
    processor = restfull()
    # Chama o método `run` da classe para executar o processo
    output = processor.run(subdirectory="Breweries")
    print("Custom Python script executed successfully.")
    return output['layer']

# Configuração da DAG
dag = DAG(
    dag_id="dag_ingestion",
    default_args={
        "owner": "Jonnathans Silva",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval="@daily",
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

# Novo step que executa um programa Python
api_step = PythonOperator(
    task_id="api_step",
    python_callable=run_restfull_api,  # Chama a função definida acima
    dag=dag,
)

bronze_job = SparkSubmitOperator(
    task_id="bronze_job",
    conn_id="spark-conn",
    application="jobs/scripts/ingestion.py",
    application_args=["--layer", "bronze"], 
    conf={
        'spark.master': 'spark://spark-master:7077',  # Use o cluster Spark
        # 'layer': "bronze", #{{ task_instance.xcom_pull(task_ids='api_step') }}
    },
    dag=dag,
)

silver_job = SparkSubmitOperator(
    task_id="silver_job",
    conn_id="spark-conn",
    application="jobs/scripts/ingestion.py",
    application_args=["--layer", "silver"], 
    conf={
        'spark.master': 'spark://spark-master:7077',  # Use o cluster Spark
    },
    dag=dag,
)

gold_job = SparkSubmitOperator(
    task_id="gold_job",
    conn_id="spark-conn",
    application="jobs/scripts/ingestion.py",
    application_args=["--layer", "gold"], 
    conf={
        'spark.master': 'spark://spark-master:7077',  # Use o cluster Spark
    },
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

# Definindo a ordem de execução
#start >> api_step >> bronze_job >> silver_job >> gold_job >> end
start >> api_step >>  bronze_job >> silver_job >> gold_job >> end