from etl.layer_bronze.upload import BaseLoader


class UploadToGold(BaseLoader):
    def __init__(self):
        super().__init__()

    def insert_layer_gold(self, row):
        schema = {
            'equipment_id': int,
            'production': float,
            'maintenance_type': str,
            'month': int,
            'year': int,
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
            'describe_month': str,
            'target_production': float
        }
        values = self.process_row(row, schema)
        columns = list(schema.keys())
        self.insert_data('layer_gold.gold_data', columns, values)
