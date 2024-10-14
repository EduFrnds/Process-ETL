import pandas as pd

from etl.layer_silver.read import ReadData


class DataTransformation(ReadData):
    def __init__(self, table_name):
        super().__init__()
        self.df = self.read_layer_bronze(table_name)

    def clean_data(self):
        df_clean = self.df.copy()
        df_clean = df_clean.dropna(subset=['production', 'hours_production', 'temperature',
                                               'operation_status', 'maintenance_type', 'hours_maintenance',
                                               'pressure', 'speed', 'vibration_level'])
        return df_clean

    # def normalize_data(self, df):
    #     """Método para normalização de colunas numéricas."""
    #     df['temperature_normalized'] = (df['temperature'] - df['temperature'].min()) / (
    #             df['temperature'].max() - df['temperature'].min()
    #     )
    #     df['pressure_normalized'] = (df['pressure'] - df['pressure'].min()) / (
    #             df['pressure'].max() - df['pressure'].min()
    #     )
    #     return df
    @staticmethod
    def aggregate_data(df_clean):

        df_clean['hours_production'] = pd.to_datetime(df_clean['hours_production'])

        df_clean['month'] = df_clean['hours_production'].dt.month
        df_clean['year'] = df_clean['hours_production'].dt.year

        df_grouped = df_clean.sort_values(by=['equipment_id', 'month']).reset_index(drop=True)

        return df_grouped

    @staticmethod
    def filter_data(df_grouped):
        """Filtra os dados para manter apenas horas de produção <= 8 horas."""

        # Converter a coluna 'hours_production' para o tipo datetime, se necessário
        df_grouped['hours_production'] = pd.to_datetime(df_grouped['hours_production'], errors='coerce')

        # Extrair apenas as horas como fração (horas + minutos/60) diretamente de 'hours_production'
        df_grouped['hours_only'] = (
                    df_grouped['hours_production'].dt.hour + df_grouped['hours_production'].dt.minute / 60
        ).round(2)
        # Filtrar os registros onde a produção seja <= 8 horas
        df_filtered = df_grouped[df_grouped['hours_only'] < 8]

        return df_filtered


if __name__ == '__main__':
    bronze_table_name = 'bronze_data'
    data_transformation = DataTransformation(bronze_table_name)
    df_cleaned = data_transformation.clean_data()
    df_aggregated = DataTransformation.aggregate_data(df_cleaned)
    df_filt = DataTransformation.filter_data(df_aggregated)
    df_filt.to_csv('silver_data.csv', index=False)
    print(df_filt)
