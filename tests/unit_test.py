import unittest
from unittest.mock import patch, MagicMock, mock_open
import os, sys
import pandas as pd
from datetime import datetime

# Adicione o diretório base do projeto ao sys.path
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(base_path)

# Agora você pode importar a classe corretamente
from jobs.scripts import DirectoryManager, DataProcessor

class TestDirectoryManager(unittest.TestCase):

    def setUp(self):
        self.directory_manager = DirectoryManager(base_path="data/transient", default_dir="Breweries")

    @patch("os.path.exists")
    @patch("os.listdir")
    def test_get_latest_path_bronze(self, mock_listdir, mock_exists):
        # Mock para simular a existência do diretório e subdiretórios
        mock_exists.return_value = True
        mock_listdir.return_value = ["2023-10-01", "2023-10-02", "2023-09-30"]

        # Chama o método
        latest_path = self.directory_manager.get_latest_path(layer="bronze")

        # Verifica se o caminho mais recente foi retornado corretamente
        self.assertIn("2023-10-02", latest_path)

    @patch("os.path.exists")
    @patch("os.listdir")
    def test_get_latest_path_silver(self, mock_listdir, mock_exists):
        # Mock para simular a existência do diretório e arquivos Parquet
        mock_exists.return_value = True
        mock_listdir.return_value = ["dat_process=2023-10-01", "dat_process=2023-10-02"]

        # Chama o método
        latest_path = self.directory_manager.get_latest_path(layer="silver")

        # Verifica se o caminho mais recente foi retornado corretamente
        self.assertIn("dat_process=2023-10-02", latest_path)

    @patch("os.path.exists")
    @patch("os.listdir")
    def test_get_latest_path_gold(self, mock_listdir, mock_exists):
        # Mock para simular a existência do diretório e subdiretórios
        mock_exists.return_value = True
        mock_listdir.side_effect = [
            ["dat_process=2023-10-01", "dat_process=2023-10-02"],  # Diretórios de data
            ["location1", "location2"]  # Subdiretórios de localização
        ]

        # Chama o método
        latest_path = self.directory_manager.get_latest_path(layer="gold")

        # Verifica se o caminho mais recente foi retornado corretamente
        self.assertIn("dat_process=2023-10-02", latest_path)

    def test_is_valid_date(self):
        # Testa datas válidas e inválidas
        self.assertTrue(self.directory_manager.is_valid_date("2023-10-01"))
        self.assertFalse(self.directory_manager.is_valid_date("invalid-date"))


class TestDataProcessor(unittest.TestCase):

    def setUp(self):
        self.directory_manager = DirectoryManager(base_path="data/transient", default_dir="Breweries")
        self.data_processor = DataProcessor(directory_manager=self.directory_manager, layer="bronze")

    @patch("pandas.read_parquet")
    @patch("os.walk")
    def test_read_data_gold(self, mock_walk, mock_read_parquet):
        # Mock para simular os arquivos Parquet na camada gold
        mock_walk.return_value = [
            ("data/transient/Breweries/dat_process=2023-10-02/location1", [], ["file1.parquet", "file2.parquet"]),
            ("data/transient/Breweries/dat_process=2023-10-02/location2", [], ["file3.parquet"])
        ]
        mock_read_parquet.side_effect = [
            pd.DataFrame({"col1": [1, 2]}),
            pd.DataFrame({"col1": [3, 4]}),
            pd.DataFrame({"col1": [5, 6]})
        ]

        # Chama o método
        df = self.data_processor.read_data(spark=None)

        # Verifica se o DataFrame final foi concatenado corretamente
        self.assertEqual(len(df), 6)
        self.assertIn("col1", df.columns)

    @patch("pandas.DataFrame.to_parquet")
    @patch("os.makedirs")
    @patch("os.path.exists")
    def test_save_gold_data(self, mock_exists, mock_makedirs, mock_to_parquet):
        # Mock para simular a existência do diretório
        mock_exists.return_value = False

        # DataFrame de exemplo
        df = pd.DataFrame({
            "type": ["brewery1", "brewery2"],
            "city": ["city1", "city2"],
            "brewery_count": [10, 20]
        })

        # Chama o método
        self.data_processor.save_gold_data(df)

        # Verifica se o diretório foi criado e o arquivo Parquet foi salvo
        mock_makedirs.assert_called_once()
        mock_to_parquet.assert_called_once()

    @patch("pandas.DataFrame.to_parquet")
    @patch("os.makedirs")
    @patch("os.path.exists")
    def test_save_data_bronze(self, mock_exists, mock_makedirs, mock_to_parquet):
        # Mock para simular a existência do diretório
        mock_exists.return_value = False

        # DataFrame de exemplo
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })

        # Chama o método
        self.data_processor.save_data(df, use_pandas=True)

        # Verifica se o diretório foi criado e o arquivo Parquet foi salvo
        mock_makedirs.assert_called_once()
        mock_to_parquet.assert_called_once()

    def test_normalize_column(self):
        # DataFrame de exemplo
        df = pd.DataFrame({
            "name": ["Brewery & Co", "Another Brewery"],
            "city": ["City1", "City2"]
        })

        # Chama o método
        self.data_processor.normalize_column(df, "name")

        # Verifica se a coluna foi normalizada corretamente
        self.assertEqual(df["name"].iloc[0], "Brewery and Co")
        self.assertEqual(df["name"].iloc[1], "Another Brewery")


if __name__ == "__main__":
    unittest.main()