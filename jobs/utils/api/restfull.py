import os
import requests
import csv
import re
import json
from datetime import datetime

class DataProcessor:
    def __init__(self, base_path=None, log_path=None):
        """
        Inicializa a classe com os caminhos base e de log.
        """
        self.base_path = base_path or os.getenv("BASE_PATH", "/opt/airflow/data/transient")
        self.log_path = log_path or os.getenv("LOG_PATH", "/opt/airflow/data/log")
        os.makedirs(self.base_path, exist_ok=True)
        os.makedirs(self.log_path, exist_ok=True)

    @staticmethod
    def validate_subdirectory(subdirectory):
        """
        Valida o nome do subdiretório para garantir que:
        - Não contenha espaços.
        - Não contenha caracteres especiais.
        - Tenha no máximo 30 caracteres.
        """
        pattern = r"^[a-zA-Z0-9_-]{1,30}$"  # Permite letras, números, _ e -, com até 30 caracteres
        if not re.match(pattern, subdirectory):
            raise ValueError("O subdiretório deve conter apenas letras, números, '_' ou '-', e ter no máximo 30 caracteres.")

    @staticmethod
    def fetch_paginated_data(api_url):
        """
        Busca todos os registros paginados de uma API.
        """
        all_data = []
        page = 1
        total_count = 0  # Contador total de registros

        while True:
            try:
                response = requests.get(f"{api_url}?page={page}&per_page=50")
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                all_data.extend(data)
                total_count += len(data)

                if total_count % 100 == 0:
                    print(f"Processados {total_count} registros até agora...")

                page += 1
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar dados da API: {e}")
                break

        print(f"Total de registros processados: {total_count}")
        return all_data

    def save_schema(self, data, subdirectory):
        """
        Salva o esquema dos dados em um arquivo JSON no subdiretório 'schema'.
        """
        schema_directory = os.path.join(self.base_path, subdirectory, "schema")
        os.makedirs(schema_directory, exist_ok=True)

        current_date = datetime.now().strftime("%Y-%m-%d")
        schema_file_path = os.path.join(schema_directory, f"{current_date}.json")

        if data:
            schema = {key: type(value).__name__ for key, value in data[0].items()}

            if os.path.exists(schema_file_path):
                with open(schema_file_path, "r", encoding="utf-8") as existing_file:
                    existing_schema = json.load(existing_file)
                    if schema == existing_schema:
                        print("O esquema não mudou. Nenhum novo arquivo será criado.")
                        return

            with open(schema_file_path, "w", encoding="utf-8") as schema_file:
                json.dump(schema, schema_file, indent=4, ensure_ascii=False)
            print(f"Esquema salvo em: {schema_file_path}")

    def save_data_to_file(self, subdirectory):
        """
        Salva os dados buscados da API em um arquivo CSV e gera um arquivo de log.
        """
        self.validate_subdirectory(subdirectory)

        current_date = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")

        directory = os.path.join(self.base_path, subdirectory, current_date)
        os.makedirs(directory, exist_ok=True)

        file_name = f"{timestamp}.csv"
        file_path = os.path.join(directory, file_name)

        api_url = "https://api.openbrewerydb.org/v1/breweries"
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = self.fetch_paginated_data(api_url)
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.save_schema(data, subdirectory)

        with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            if data:
                header = data[0].keys()
                writer.writerow(header)
                for row in data:
                    writer.writerow(row.values())

        log_directory = os.path.join(self.log_path, current_date)
        os.makedirs(log_directory, exist_ok=True)

        log_file_name = f"{subdirectory}_{timestamp}.csv"
        log_file_path = os.path.join(log_directory, log_file_name)

        with open(log_file_path, "w", newline="", encoding="utf-8") as log_file:
            log_writer = csv.writer(log_file)
            log_writer.writerow(["Directory", "File Name", "File Path", "Start Time", "End Time", "Record Count"])
            log_writer.writerow([directory, file_name, file_path, start_time, end_time, len(data)])

        print(f"Dados salvos em: {file_path}")
        print(f"Log salvo em: {log_file_path}")

        return {
            "start_process": start_time,
            "end_process": end_time,
            "file_path": file_path,
            "log_file_path": log_file_path
        }

    def run(self, subdirectory="Breweries"):
        """
        Executa o processo de busca e salvamento de dados.
        """
        print("Iniciando busca de dados...")
        try:
            self.validate_subdirectory(subdirectory)
            output = self.save_data_to_file(subdirectory)
            # Adiciona a chave 'layer' com o valor 'bronze' ao dicionário de saída
            output["layer"] = "bronze"
            print(f">>>>>>> Processo concluído. Informações do arquivo: {output}")
            return output  # Retorna o resultado do processamento
        except ValueError as e:
            print(f"Erro de validação: {e}")
            return {"error": str(e)}

if __name__ == "__main__":
    processor = DataProcessor()
    processor.run()