import boto3
import logging
import pandas as pd
import tabula
import requests
from io import StringIO
from database_utils import DatabaseConnector

logging.basicConfig(level=logging.INFO)

# weblinks to files for extraction
s3_address = 's3://data-handling-public/products.csv' 
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

class DataExtractor:     
    
    def __init__(self, db_connector, api_config):
        """
        Initialises the DataExtractor instance.

        Parameters:
        - db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
        - api_config (dict): A dictionary containing API configuration details.
        """
        try:
            self.db_connector = db_connector
            #Task 5  dictionary
            self.api_config = api_config
        except Exception as e:
            logging.error(f' data_extraction.py -- 0.0 failure to initialise connector and api configuration: {e}')
 
    def read_rds_table(self, table_name):
        """
        Reads data from an RDS table.

        Parameters:
        - table_name (str): Name of the RDS table.

        Returns:
        - pd.DataFrame: A DataFrame containing the data from the specified RDS table.
        """
        try:
            """ Read datas from an RDS  table. """
            extracted_table  = f"SELECT * FROM {table_name};"
            return pd.read_sql(extracted_table, self.db_connector.engine1)
        except Exception as e:
            logging.error(f'  data_exptration.py -- 1.0 failure to extract data from rds table: {e}')
    
    def retrieve_pdf_data(self, pdf_url):
        """
        Converts  pdf file into a pandas DataFrame.

        Parameters:
        - pdf_url in string type: URL of the PDF file.

        Returns:
        - pandas dataframe with the extracted data from the PDF.
        """
        try: 
            card_data = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
            combined_data = pd.concat(card_data, ignore_index=True)
            return combined_data
        except Exception as e:
            logging.error(f'    data_extraction.py -- 2.0 failure to extract data from pdf files: {e}')

    def list_number_of_stores(self):
        """
        Lists the number of stores.

        Returns:
        - Integer: The number of stores.
        """
        try: 
            response = requests.get(self.api_config['number_of_stores_endpoint'], headers=self.api_config['headers'])
            logging.info('  data_extraction.py -- obtaining number of stores')
            return response.json()['number_stores']
        except Exception as e:
            logging.error(f'    data_extraction.py -- 3.1 failure to list number of stores: {e}')

    def retrieve_stores_data(self, store_details_endpoint, number_of_stores):
        """
        Retrieves data for multiple stores.

        Parameters:
        - store_details_endpoint (string): The API endpoint for store details.
        - number_of_stores (integer): The number of stores to retrieve data for.

        Returns:
        - pandas dataframe with data for multiple stores.
        """
        try:
            stores_data = []
            failed_stores = []  
            for store_number in range(0, number_of_stores):
                stores_url = f"{store_details_endpoint}{store_number}"
                response = requests.get(stores_url, headers=self.api_config['headers'])
                try:
                    if response.status_code == 200:
                        store_data = response.json()
                        stores_data.append(store_data)
                    else: 
                        failed_stores.append(store_number)
                except requests.RequestException as req_ex:
                    logging.warning(f'failed to process request {req_ex}')
                except Exception as e:
                    logging.error(f' EXTRACT -- 4.1 retrieve_stores_data -- pd dataframe and appending store data failed: {e}')
            if failed_stores:
                logging.warning(f'failed to retrieve data for {len(failed_stores)} stores: {failed_stores}')
            stores_table = pd.DataFrame(stores_data)
            stores_table.to_csv('stores_final.csv', index=False)
            return stores_table
        except Exception as e:
            logging.error(f'    data_extraction.py -- 3.2 failure to extract stores data from json: {e}')
            return pd.DataFrame()

        
    def extract_from_s3(self, s3_address):
        """
        Extracts data from an S3 bucket.

        Parameters:
        - s3_address (string): The address for the S3 bucket.

        Returns:
        - pandas dataframe with data from the S3 bucket.
        """
        try: 
            bucket_name, object_key = s3_address.replace('s3://', '').split('/', 1)
            # Initialize S3 client
            s3 = boto3.client('s3')
            logging.info(' data_extarction.py -- bucket initialised')
            # Download CSV file from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            s3_csv_content = response['Body'].read().decode('utf-8')
            logging.info('data_extraction.py -- csv content decoded')
            # Convert CSV content to Pandas DataFrame
            df = pd.read_csv(StringIO(s3_csv_content)) 
            logging.info('pandas csv created') 
            # Convert json  content to Pandas DataFrame
            return df
        except Exception as e:
            logging.error(f'    data_extraction.py -- 4.0 failure to extract data from s3 bucket: {e}')

    def extract_json_from_url(self, json_url):
        """
        Extracts data from a JSON file at the specified URL.

        Parameters:
        - json_url (string): The URL of the JSON file.

        Returns:
        - pandas datafram with data from the JSON file.
        """

        try:
            response = requests.get(json_url)
            if response.status_code == 200:
                json_content =response.json()
                df =pd.DataFrame(json_content)
                return df
            else: 
                logging.error(f'    data_extraction -- 6.0 failed to connect to pdf URL {json_url}. Status code: {response.status_code}')
                return None
        except requests.exceptions.RequestException as req_err:
            logging.error(f'Error connecting to json url: {json_url}. Error: {req_err}')
            return None
        except Exception as e:
            logging.error(f'    data_extraction.py -- 6 failure to extract json from pdf url: {e}')
            return None