from etl.data_manager import BaseLoader


class UploadToBronze(BaseLoader):
    def __init__(self):
        super().__init__()

    def insert_layer_bronze(self, row):
        schema = {
            'equipment_id': int,
            'production': float,
            'hours_production': str,
            'temperature': float,
            'pressure': float,
            'speed': float,
            'vibration_level': float,
            'operation_status': str,
            'maintenance_type': str,
            'hours_maintenance': float
        }
        values = self.process_row(row, schema)
        columns = list(schema.keys())
        self.insert_data('layer_bronze.bronze_data', columns, values)
