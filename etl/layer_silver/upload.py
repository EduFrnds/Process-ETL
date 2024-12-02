from etl.data_manager import BaseLoader


class UploadToSilver(BaseLoader):
    def __init__(self):
        super().__init__()

    def insert_layer_silver(self, row):
        schema = {
            'equipment_id': int,
            'production': float,
            'hours_production': str,
            'maintenance_type': str,
            'maintenance_minutes': float,
            'production_minutes': float,
            'temperature_mean': float,
            'vibration_level_mean': float,
            'temperature_std': float,
            'vibration_level_std': float,
            'temperature_lsc': float,
            'temperature_lsi': float,
            'vibration_level_lsc': float,
            'vibration_level_lsi': float,
            'temperature': float,
            'vibration_level': float,
            'pressure': float
        }
        values = self.process_row(row, schema)
        columns = list(schema.keys())
        self.insert_data('layer_silver.silver_data', columns, values)
