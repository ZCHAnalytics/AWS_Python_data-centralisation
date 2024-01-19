#data_extraction.py 
import pandas as pd #T3 S5
import tabula
from database_utils import DatabaseConnector #T3 S5
import requests #Task 5
import boto3 #Task 6
from io import StringIO

class DataExtractor: # Task2 utility class to extract data from different sources 
    def __init__(self, db_connector):
        self.db_connector = db_connector
        #Task 5 Creating a dictionary
        self.api_config = {
            'headers': {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'},
            'number_of_stores_endpoint': 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',
            'store_details_endpoint': 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        }
    
    def read_rds_table(self, table_name):
        extracted_table  = f"SELECT * FROM {table_name};"
        return pd.read_sql(extracted_table, self.db_connector.engine1)
    
    def retrieve_pdf_data(self, pdf_url):
        card_data = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
        combined_data = pd.concat(card_data, ignore_index=True)
        return combined_data
    
    #Task 5
    def list_number_of_stores(self):
        response = requests.get(self.api_config['number_of_stores_endpoint'], headers=self.api_config['headers'])
        return response.json()['number_stores']
    
    # Use the list_number_of_stores method
    def retrieve_stores_data(self, store_details_endpoint, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            store_url = f"{store_details_endpoint}{store_number}"
            response = requests.get(store_url, headers=self.api_config['headers'])

            if response.status_code == 200:
                #Access the response data as JSON
                store_data = response.json()
                stores_data.append(store_data)

        df = pd.DataFrame(stores_data)
        return df
        #Task 6
    def extract_from_s3(self, s3_address):
        # Splitting the S3 address to get bucket name and object key
        bucket_name, object_key = s3_address.replace('s3://', '').split('/', 1)
        # Initialize S3 client
        
        s3 = boto3.client('s3')
        # Download CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        # Convert CSV content to Pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))
        # Convert json  content to Pandas DataFrame
        return df
    def extract_json_from_url(self, json_url):
        try:
            response = requests.get(json_url)
            if response.status_code == 200:
                json_content =response.json()
                df =pd.DataFrame(json_content)
                return df
            else: 
                print(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error during JSON extraction: {e}")
            return None
        
db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)

json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
extracted_dates = data_extractor.extract_json_from_url(json_url)
csv_file_path = '/Users/Anaya/OneDrive/Documents/AICore/MRDC/dates.csv'
extracted_dates.to_csv('/Users/Anaya/OneDrive/Documents/AICore/MRDC/dates.csv')

"""
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_stores = data_extractor.list_number_of_stores()
store_details_endpoint = data_extractor.api_config['store_details_endpoint']
extracted_store_data = data_extractor.retrieve_stores_data(store_details_endpoint, number_of_stores)


s3_address = 's3://data-handling-public/products.csv'
extracted_s3 = data_extractor.extract_from_s3(s3_address)
details = extracted_s3.info()
#print(details)


table_name = 'orders_table'
order_df = data_extractor.read_rds_table(table_name)
csv_file_path = '/Users/Anaya/OneDrive/Documents/AICore/MRDC/orders.csv'
order_df.to_csv('/Users/Anaya/OneDrive/Documents/AICore/MRDC/orders.csv')

"""

