import pandas as pd

from etl.data_manager import DataManager
from etl.layer_gold.read import ReadDataSilver


class DataTransformationGold(ReadDataSilver):
    def __init__(self, table_name, data_path):
        super().__init__()
        self.df = self.read_layer_silver(table_name)
        self.data_manager = DataManager(data_path)

    @staticmethod
    def aggregate_data(df, headers):
        df['describe_month'] = df['month'].apply(lambda x: pd.to_datetime(x, format='%m').strftime('%B'))
        df['target_production'] = 500

        gold_data = pd.DataFrame(df)
        gold_data.to_csv('./data/gold_data.csv', index=False)

        return gold_data
