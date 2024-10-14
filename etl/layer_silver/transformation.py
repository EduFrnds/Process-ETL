from etl.layer_silver.read import ReadData


class DataTransformation(ReadData):
    def __init__(self, table_name):
        super().__init__()
        self.df = self.read_layer_bronze(table_name)

    def transform_data(self):
        if self.df is None or self.df.empty:
            print("O DataFrame está vazio ou não foi carregado corretamente.")
            return None

        df_cleaned = self.df.copy()
        df_production = df_cleaned[['equipment_id', 'production', 'hours_production',
                                    'temperature', 'pressure', 'speed',
                                    'vibration_level', 'operation_status', 'maintenance_type', 'hours_maintenance']]
        print(df_production.head())
        return df_production


if __name__ == '__main__':
    bronze_table_name = 'bronze_data'
    data_transformation = DataTransformation(bronze_table_name)
    data_transformation.transform_data()
