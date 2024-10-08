import csv


class DataManager:
    def __init__(self, file_path):
        self.file_path = '../data'
        self.path = file_path

    def save_to_csv(self, data, filename, headers):
        """Save data to CSV file"""
        file_path = f"{self.path}/{filename}.csv"
        with open(file_path, 'wt', newline='', encoding='utf-8') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
