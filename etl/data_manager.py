import pandas as pd


class DataManager:
    def __init__(self, file_path):
        self.file_path = './data'
        self.path = file_path

    def save_to_parquet(self, data, filename):
        """Save data to parquet file."""
        df = pd.DataFrame(data)
        df.to_parquet(f"{self.file_path}/{filename}.parquet", index=False)

    def read_parquet(self, filename):
        """Read data from parquet file."""
        return pd.read_parquet(f"{self.file_path}/{filename}.parquet")
