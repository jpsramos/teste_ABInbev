import os
from datetime import datetime

def create_file_in_data_directory():
    """
    Cria um arquivo no formato yyyymmdd.txt no diretório ./data.
    """
    # Obtém a data atual no formato yyyymmdd
    current_date = datetime.now().strftime("%Y%m%d")
    file_name = f"{current_date}.txt"
    
    # Define o diretório base
    base_path = "/opt/airflow/data/bla"
    
    # Garante que o diretório ./data exista
    os.makedirs(base_path, exist_ok=True)
    
    # Caminho completo do arquivo
    file_path = os.path.join(base_path, file_name)
    
    # Conteúdo do arquivo
    content = "Arquivo gerado com sucesso"
    
    # Cria e escreve no arquivo
    with open(file_path, "w", encoding="utf-8") as txt_file:
        txt_file.write(content)
    
    print(f"Arquivo criado em: {file_path}")

def main():
    """
    Função principal que chama o método para criar o arquivo.
    """
    create_file_in_data_directory()

if __name__ == "__main__":
    main()