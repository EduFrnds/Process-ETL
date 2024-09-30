from etl.extract_data import ExtractData


class ETLProcess:
    def run_etl(self):
        # 1. Extração de dados (pode ser de fontes geradas ou externas)
        extract_data = ExtractData()
        data = extract_data.generate_data_customers(10)
        print(data)


if __name__ == '__main__':
    etl_process = ETLProcess()
    etl_process.run_etl()
