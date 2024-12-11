import os, sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime
import argparse

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

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_path)

class DirectoryManager:
    """
    Classe para gerenciar diretórios e validações relacionadas.
    """

    def __init__(self, base_path="data/transient", default_dir="Breweries"):
        """
        Inicializa a classe com o caminho base e o diretório padrão.
        """
        self.base_path = base_path
        self.default_dir = default_dir

    def get_latest_path(self, layer="bronze"):
        """
        Busca o caminho mais recente (diretório ou arquivo) com base na camada.
        Método genérico que pode ser usado para diferentes camadas.
        """
        try:
            # Define o caminho base para a camada especificada
            if layer == "bronze":
                full_path = os.path.join(self.base_path, self.default_dir)
            elif layer == "silver":
                # O caminho base para silver é derivado do bronze
                full_path = os.path.join(self.base_path.replace("transient", "bronze"), self.default_dir)
            elif layer == "gold":
                # Busca os diretórios 'dat_process=YYYY-MM-DD'
                full_path = os.path.join(self.base_path.replace("transient", "silver"), self.default_dir)
                subdirs = [d for d in os.listdir(full_path) if d.startswith("dat_process=")]
                print(f"######## {full_path}")
                if not subdirs:
                    raise ValueError("Nenhum diretório 'dat_process=YYYY-MM-DD' encontrado.")
                
                # Seleciona o diretório mais recente
                latest_dir = max(subdirs, key=lambda d: d.split("=")[-1])
                latest_dir_path = os.path.join(full_path, latest_dir)

                # Itera sobre os subdiretórios de localização
                location_dirs = [os.path.join(latest_dir_path, loc) for loc in os.listdir(latest_dir_path) if os.path.isdir(os.path.join(latest_dir_path, loc))]
                if not location_dirs:
                    raise ValueError("Nenhum subdiretório de localização encontrado na camada gold.")

                # Retorna o caminho do diretório mais recente
                return latest_dir_path
            else:
                raise ValueError("Camada inválida. Escolha entre 'bronze' ou 'silver'.")

            # Verifica se o caminho existe
            if not os.path.exists(full_path):
                raise ValueError(f"O diretório base '{full_path}' não existe para a camada {layer}.")

            # Para a camada bronze, busca o diretório mais recente
            if layer == "bronze":
                subdirs = [d for d in os.listdir(full_path) if os.path.isdir(os.path.join(full_path, d))]
                date_dirs = [d for d in subdirs if self.is_valid_date(d)]
                if not date_dirs:
                    raise ValueError("Nenhum diretório com formato de data encontrado.")
                latest_dir = max(date_dirs, key=lambda d: datetime.strptime(d, "%Y-%m-%d"))
                return os.path.join(full_path, latest_dir)

            # Para a camada silver, busca o arquivo mais recente no diretório da camada bronze
            elif layer == "silver":
                subdirs = [d for d in os.listdir(full_path) if d.startswith("dat_process=")]
                if not subdirs:
                    raise ValueError("Nenhum diretório 'dat_process=YYYY-MM-DD' encontrado.")
                latest_dir = max(subdirs, key=lambda d: d.split("=")[-1])
                latest_dir_path = os.path.join(full_path, latest_dir)

                parquet_files = [f for f in os.listdir(latest_dir_path) if f.startswith("output_") and f.endswith(".parquet")]
                if not parquet_files:
                    raise ValueError("Nenhum arquivo Parquet com o prefixo 'output_' encontrado.")
                latest_file = max(parquet_files, key=lambda f: f.split("_")[1].split(".")[0])
                print(os.path.join(latest_dir_path, latest_file))
                return os.path.join(latest_dir_path, latest_file)
            # Para a camada gold, busca o arquivo mais recente no diretório da camada bronze
            elif layer == "gold":
                print("########################### {full_path}")
                subdirs = [d for d in os.listdir(full_path) if d.startswith("dat_process=")]
                if not subdirs:
                    raise ValueError("Nenhum diretório 'dat_process=YYYY-MM-DD' encontrado.")
                print(">>>>>>>>>>>>>>>>>>>>> {subdirs}")
                latest_dir = max(subdirs, key=lambda d: d.split("=")[-1])
                print("ÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇÇ {latest_dir}")
                latest_dir_path = os.path.join(full_path, latest_dir)
                print("@@@@@@@@@@@@@@@@@@@@@@@@ {latest_dir_path}")
                parquet_files = [f for f in os.listdir(latest_dir_path) if f.startswith("output_") and f.endswith(".parquet")]
                if not parquet_files:
                    raise ValueError("Nenhum arquivo Parquet com o prefixo 'output_' encontrado.")
                latest_file = max(parquet_files, key=lambda f: f.split("_")[1].split(".")[0])
                print(os.path.join(latest_dir_path, latest_file))
                return os.path.join(latest_dir_path, latest_file)

        except Exception as e:
            raise RuntimeError(f"Erro ao buscar o caminho mais recente: {e}")

    @staticmethod
    def is_valid_date(date_str):
        """
        Verifica se uma string está no formato de data YYYY-MM-DD.
        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False


class DataProcessor:
    """
    Classe para processar dados utilizando PySpark e Pandas.
    """

    def __init__(self, directory_manager: DirectoryManager, layer="bronze"):
        """
        Inicializa a classe com uma instância de DirectoryManager e define a camada padrão.
        """
        self.directory_manager = directory_manager
        self.layer = layer  # Define a camada padrão como "bronze"

    @staticmethod
    def normalize_column(df, column_name):
        """
        Normaliza os valores de uma coluna específica do DataFrame Pandas.
        Substitui caracteres especiais e normaliza para NFKC.
        """
        import unicodedata

        def normalize_string(value):
            if isinstance(value, str):
                # Remove caracteres especiais e normaliza para NFKC
                normalized = unicodedata.normalize('NFKC', value)
                # Substitui caracteres indesejados, como "&" por "and"
                cleaned = normalized.replace("&", "and")
                return cleaned
            return value

        if column_name in df.columns:
            df[column_name] = df[column_name].apply(normalize_string)
        else:
            raise ValueError(f"A coluna '{column_name}' não existe no DataFrame.")

    def read_data(self, spark):
        """
        Lê dados da camada especificada (bronze, silver ou gold).
        Método genérico para leitura de dados, adaptável para diferentes camadas.
        """
        try:
            latest_path = self.directory_manager.get_latest_path(layer=self.layer)
            print(f"@@@@@@@@@@@@@@@@@@ {latest_path}")
            
            if self.layer == "bronze":
                # Leitura dos arquivos CSV com PySpark
                csv_file_path = os.path.join(latest_path, "*.csv")
                df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_file_path)

                if df.count() == 0:
                    raise ValueError("O DataFrame está vazio. Verifique os arquivos de entrada.")

                print(f"Arquivos CSV lidos do diretório: {latest_path}")
                df.printSchema()
                return df

            elif self.layer == "silver":
                # Leitura do arquivo Parquet com Pandas
                df = pd.read_parquet(latest_path, engine="pyarrow")
                print(f"Arquivo Parquet lido do caminho: {latest_path}")
                return df

            elif self.layer == "gold":
                print(f"################### {latest_path}")

                # Leitura da camada gold com duas partições (data e localização)
                dataframes = []
                for root, dirs, files in os.walk(latest_path):
                    for file in files:
                        if file.endswith(".parquet"):
                            file_path = os.path.join(root, file)
                            # Lê o arquivo Parquet e adiciona ao DataFrame
                            df = pd.read_parquet(file_path, engine="pyarrow")
                            dataframes.append(df)

                # Concatena todos os DataFrames em um único DataFrame
                if dataframes:
                    final_df = pd.concat(dataframes, ignore_index=True)
                    print(f"Dados lidos da camada gold: {latest_path}")
                    return final_df
                else:
                    raise ValueError("Nenhum arquivo Parquet encontrado na camada gold.")

        except Exception as e:
            raise RuntimeError(f"Erro ao ler os dados da camada {self.layer}: {e}")

    def save_gold_data(self, df):
        """
        Salva o DataFrame no formato Parquet na camada gold.
        Cria uma visão agregada com a quantidade de cervejarias por tipo e localização.
        Remove arquivos anteriores para a mesma partição antes de salvar.
        """
        try:
            # Verifica se o DataFrame é válido
            if df is None or df.empty:
                raise ValueError("O DataFrame fornecido está vazio ou é None.")

            # Define a data atual para a partição
            current_date = datetime.now().strftime("%Y-%m-%d")
            output_base_path = self.directory_manager.base_path.replace("transient", "gold")
            output_path = os.path.join(output_base_path, self.directory_manager.default_dir, f"dat_process={current_date}")

            # Cria o diretório se não existir
            if not os.path.exists(output_path):
                os.makedirs(output_path, exist_ok=True)
                print(f"Diretório criado: {output_path}")

            # Remove arquivos existentes no diretório
            self._clear_directory(output_path)

            # Cria a visão agregada com a quantidade de cervejarias por tipo e localização
            aggregated_df = (
                df.groupby(["type", "city"])
                .size()
                .reset_index(name="brewery_count")
            )

            # Adiciona colunas de data e timestamp
            aggregated_df["dat_process"] = current_date
            aggregated_df["dat_time_process"] = datetime.now()

            # Gera o nome do arquivo no formato YYYYMMDD_HHMISS
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pandas_file_path = os.path.join(output_path, f"output_{timestamp}.parquet")

            # Salva o DataFrame Pandas no formato Parquet
            aggregated_df.to_parquet(pandas_file_path, engine="pyarrow", index=False)
            print(f"Arquivo salvo na camada gold no caminho: {pandas_file_path}")

        except Exception as e:
            raise RuntimeError(f"Erro ao salvar os dados na camada gold: {e}")

        except Exception as e:
            raise RuntimeError(f"Erro ao salvar os dados na camada {self.layer}: {e}")

    def save_data(self, df, use_pandas=True):
        """
        Salva o DataFrame no formato Parquet.
        Método genérico para salvar dados, adaptável para diferentes camadas.
        """
        try:
            if self.layer == "gold":
                # Chama o método específico para salvar na camada gold
                self.save_gold_data(df)
                return

            current_date = datetime.now().strftime("%Y-%m-%d")
            output_base_path = self.directory_manager.base_path.replace("transient", self.layer)
            output_path = os.path.join(output_base_path, self.directory_manager.default_dir, f"dat_process={current_date}")
            print(f">>>>>>>>>>>>>>>>>>>>>>>> {output_path}")
            if not os.path.exists(output_path):
                os.makedirs(output_path, exist_ok=True)
                print(f"Diretório criado: {output_path}")

            if self.layer == "bronze":
                # Adiciona colunas de data e timestamp no PySpark DataFrame
                df = df.withColumn("dat_process", lit(current_date))
                df = df.withColumn("dat_time_process", current_timestamp())

                if use_pandas:
                    # Converte para Pandas
                    pandas_df = df.toPandas()

                    # Remove arquivos existentes no diretório
                    self._clear_directory(output_path)

                    # Gera o nome do arquivo no formato YYYYMMDD_HHMISS
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    pandas_file_path = os.path.join(output_path, f"output_{timestamp}.parquet")

                    # Salva como Parquet usando Pandas
                    pandas_df.to_parquet(pandas_file_path, engine="pyarrow", index=False)
                    print(f"Arquivo salvo com Pandas no caminho: {pandas_file_path}")
                else:
                    # Salva diretamente com PySpark no formato Parquet com sobrescrita
                    df.write.mode("overwrite").parquet(output_path)
                    print(f"Arquivo salvo com PySpark no caminho: {output_path}")

            elif self.layer == "silver":
                # Verifica e adiciona as colunas ao DataFrame Pandas
                if "dat_process" not in df.columns:
                    df["dat_process"] = current_date
                if "dat_time_process" not in df.columns:
                    df["dat_time_process"] = datetime.now()

                # Renomeia as colunas com base no dicionário de mapeamento
                df.rename(columns=column_mapping, inplace=True)

                # Agrupa os dados por 'dat_process' e 'city'
                grouped = df.groupby(["dat_process", "city"])
                for (dat_process, city), group in grouped:
                    # Define o caminho base para a partição
                    partition_path = os.path.join(output_path, f"city={city}")
                    print(f"********************* {partition_path}")
                    print(f"======================= {output_path}")
                    os.makedirs(partition_path, exist_ok=True)

                    # Remove arquivos existentes no diretório da partição
                    self._clear_directory(partition_path)

                    # Gera o nome do arquivo no formato YYYYMMDD_HHMISS
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    pandas_file_path = os.path.join(partition_path, f"output_{timestamp}.parquet")
                    print(f"+++++++++++++++++ {pandas_file_path}")
                    # Salva o DataFrame Pandas no formato Parquet
                    group.to_parquet(pandas_file_path, engine="pyarrow", index=False)
                    print(f"Arquivo salvo com Pandas no caminho: {pandas_file_path}")

        except Exception as e:
            raise RuntimeError(f"Erro ao salvar os dados na camada {self.layer}: {e}")

    @staticmethod
    def _clear_directory(path):
        """
        Remove todos os arquivos existentes no diretório especificado.
        Método auxiliar para evitar duplicação de código.
        """
        if os.path.exists(path):
            for file in os.listdir(path):
                file_path = os.path.join(path, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print(f"Arquivos antigos removidos do diretório: {path}")

    @staticmethod
    def main():
        """
        Método principal para executar o processamento de dados.
        """
        try:
            # Criação da SparkSession
            spark = SparkSession.builder \
                .appName("Read and Write Data Example") \
                .getOrCreate()

            parser = argparse.ArgumentParser(description="Processamento de dados com Spark")
            parser.add_argument("--layer", type=str, default="bronze", help="Camada de dados a ser processada (bronze ou silver)")
            args = parser.parse_args()

            # Captura o valor do argumento 'layer'
            layer = args.layer 
            print(f"Camada recebida: {layer}")

            # Instancia a classe DirectoryManager
            directory_manager = DirectoryManager()

            # Instancia a classe DataProcessor com camada padrão "silver"
            processor = DataProcessor(directory_manager, layer=layer)

            # Leitura dos dados
            df = processor.read_data(spark)

            # Salvamento no formato Parquet
            processor.save_data(df, use_pandas=True)

        except Exception as e:
            print(f"Erro no processamento: {e}")
        finally:
            # Finaliza a SparkSession
            spark.stop()


# Exemplo de uso
if __name__ == "__main__":
    DataProcessor.main()