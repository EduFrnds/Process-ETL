import pandas as pd

from etl.data_manager import DataManager
from etl.layer_silver.read import ReadDataBronze


class DataTransformationSilver(ReadDataBronze):
    def __init__(self, table_name, data_path):
        super().__init__()
        self.df = self.read_layer_bronze(table_name)
        self.data_manager = DataManager(data_path)

    def clean_data(self):
        df_clean = self.df.copy()
        df_clean = df_clean.dropna(subset=['production', 'hours_production',
                                           'operation_status', 'maintenance_type', 'hours_maintenance',
                                           'pressure', 'speed', 'vibration_level'])
        return df_clean

    @staticmethod
    def aggregate_data(df_clean):
        df_clean['hours_production'] = pd.to_datetime(df_clean['hours_production'])

        df_clean['month'] = df_clean['hours_production'].dt.month
        df_clean['year'] = df_clean['hours_production'].dt.year

        df_grouped = df_clean.sort_values(by=['equipment_id', 'month']).reset_index(drop=True)

        return df_grouped

    @staticmethod
    def filter_data(df_grouped):
        # Converter a colunas
        df_grouped['hours_production'] = pd.to_datetime(df_grouped['hours_production'], errors='coerce')
        df_grouped['maintenance_minutes'] = (df_grouped['hours_maintenance'] * 60).round(2)

        # Extrair apenas as horas como fração (horas + minutos/60) diretamente de 'hours_production'
        df_grouped['hours_only'] = (
                df_grouped['hours_production'].dt.hour
                + df_grouped['hours_production'].dt.minute / 60
        ).round(2)

        df_grouped['production_minutes'] = (df_grouped['hours_only'] * 60).round(2)

        # Filtrar os registros onde a produção seja <= 8 horas
        df_filtered = df_grouped[df_grouped['hours_only'] < 8]

        # Remover as colunas
        df_filtered = df_filtered.drop(
            columns=[
                'hours_production',
                'speed',
                'operation_status',
                'hours_maintenance',
                'hours_only',
            ]
        )
        df_filtered = df_filtered.rename(columns={'minutes_maintenance': 'hours_maintenance'})

        return df_filtered

    @staticmethod
    def derive_data(df_filtered, headers):
        # Calcular a média das colunas temperature e vibration_level
        df_filtered['temperature_mean'] = (
            df_filtered.groupby('equipment_id')['temperature']
            .transform('mean')
            .round(2)
        )
        df_filtered['vibration_level_mean'] = (
            df_filtered.groupby('equipment_id')['vibration_level']
            .transform('mean')
            .round(2)
        )

        # Calcular o desvio padrão das colunas temperature e vibration_level
        df_filtered['temperature_std'] = (
            df_filtered.groupby('equipment_id')['temperature']
            .transform('std')
            .round(2)
        )
        df_filtered['vibration_level_std'] = (
            df_filtered.groupby('equipment_id')['vibration_level']
            .transform('std')
            .round(2)
        )

        # Calcular o LSC e LSI para temperature
        df_filtered['temperature_lsc'] = (
                df_filtered['temperature_mean'] + 3 * df_filtered['temperature_std']
        ).round(2)
        df_filtered['temperature_lsi'] = (
                df_filtered['temperature_mean'] - 3 * df_filtered['temperature_std']
        ).round(2)

        # Calcular o LSC e LSI para vibration_level
        df_filtered['vibration_level_lsc'] = (
                df_filtered['vibration_level_mean'] + 3 * df_filtered['vibration_level_std']
        ).round(2)
        df_filtered['vibration_level_lsi'] = (
                df_filtered['vibration_level_mean'] - 3 * df_filtered['vibration_level_std']
        ).round(2)

        # Remover colunas desnecessárias
        df_filtered = df_filtered.drop(
            columns=[
                'pressure',
                'vibration_level',
            ]
        )

        # Salvar os dados processados
        silver_data = pd.DataFrame(df_filtered)
        silver_data.to_csv('./data/silver_data.csv', index=False)

        return silver_data
