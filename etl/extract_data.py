import pandas as pd
from faker import Faker


class ExtractData:

    def __init__(self):
        """
        Constructor for ExtractData.
        This constructor will be used to initialize Faker.
        """
        self.fake = Faker()

    def generate_data_customers(self, n):
        data_customers = []

        for _ in range(n):
            data_customers.append(
                {
                    "customer_id": self.fake.random_number(n),
                    "customer_name": self.fake.name(),
                    "customer_address": self.fake.address(),
                    "customer_email": self.fake.email(),
                    "customer_phone": self.fake.phone_number(),
                    "customer_date_created": self.fake.date_time_this_century()
                }
            )
        return pd.DataFrame(data_customers)
