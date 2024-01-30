"""
File: data_extraction.py
Purpose: Diving into the data pool, taming the wilderness and extracting insights.
Author: Zulfia
Date: January 2024

# Big shoutout to the data pioneers: A., B., H., I., I., J., K., M., M., V., W., for helping me blaze the wild trail!
"""

# External Libraries
import boto3
import logging
import pandas as pd
import tabula
import requests

# Internal Libraries and Database Credentials
from database_utils import DatabaseConnector, aws_credentials_file, local_credentials_file

# Logging Configuration
logging.basicConfig(level=logging.INFO)

# External Data Links
s3_address = 's3://data-handling-public/products.csv' 
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'


# Data Extractor Class and Methods
class DataExtractor:     
    """
    Class for extracting data from various sources.

    Attributes:
    -----------
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
        api_config (dict): A dictionary containing API configuration details.

    Methods:
    -----------
        __init__(self, db_connector, api_config): Initialises the DataExtractor instance.
        def read_rds_table(self, table_name): Reads data from an RDS table.
        def retrieve_pdf_data(self, pdf_url): Converts  pdf file into a pandas DataFrame.
        def list_number_of_stores(self): Lists the number of stores.
        def retrieve_stores_data(self, store_details_endpoint, number_of_stores): Retrieves data for multiple stores.
        def extract_from_s3(self, s3_address): Extracts data from an S3 bucket.
        def extract_json_from_url(self, json_url): Extracts data from a JSON file at the specified URL.
    """

    def __init__(self, db_connector):
        """
        Initialises the DataExtractor instance.

        Parameters:
        ----------
            - db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
            - api_config (dict): A dictionary containing API configuration details.
        """
        try:
            # Assign input parameters to instance variables
            self.db_connector = db_connector
            
           # Assign API configuration dictionary
            self.api_config = db_connector.read_api_config()
    
        except Exception as e:
            logging.error(f'Error in data_extraction method __init__: {e}')
 
    def read_rds_table(self, table_name):
        """
        Reads data from an RDS table.

        Parameters:
        -----------
            - table_name (str): Name of the RDS table.

        Returns:
            - A Panda DataFrame containing the data from the specified RDS table.
        """
        
        try:
            # Build the SQL query to extract all data from the specified RDS table.
            extracted_table_query  = f"SELECT * FROM {table_name};"
            
            # Use the SQLAlchemy engine to execute the query and read the result into a Pandas DataFrame.
            df = pd.read_sql(extracted_table_query, self.db_connector.external_data_engine)
        
            # Save the DataFrame to a CSV file locally with the name {table_name}.csv        
            csv_filename = f"{table_name}.csv"
            df.to_csv(f'csv_files/{csv_filename}', index=False)
    
            return df
        
        except Exception as e:
            logging.error(f'Error in data_extraction method read_rds_table: {e}')
    
    def retrieve_pdf_data(self, pdf_url):
        """
        Converts pdf file into a Pandas DataFrame.

        Parameters:
        ----------
            - pdf_url (str): URL of the PDF file.

        Returns:
            - A Pandas DataFrame with the extracted data from the PDF.
        """
        
        try: 
             # Use the tabula library to read tables from the PDF file at the specified URL.
            card_data = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
            
            # Combine the extracted tables into a single Pandas DataFrame.
            combined_card_tables = pd.concat(card_data, ignore_index=True)

            # Save the combined data to a CSV file locally.
            combined_card_tables.to_csv('csv_files/cards_table.csv', index=False)
            
            return combined_card_tables
        
        except Exception as e:
            logging.error(f'Error in data_extraction method retrieve_pdf_data: {e}')

    def list_number_of_stores(self):
        """
        Lists the number of stores.

        Returns:
            - Integer: The number of stores.
        """

        try:
             # Send a GET request to the specified API endpoint to obtain information about the number of stores. 
            response = requests.get(self.api_config['number_of_stores_endpoint'], headers=self.api_config['headers'])

            # Extract the 'number_stores' value from the JSON response.
            return response.json()['number_stores']
        
        except Exception as e:
            logging.error(f'Error in data_extraction method list_number_of_stores: {e}')


    def retrieve_stores_data(self, store_details_endpoint, number_stores):
        """
        Retrieves data for multiple stores from an API endpoint.

        Parameters:
        ----------
            - store_details_endpoint (string): The API endpoint for store details.
            - number_stores (integer): The number of stores to retrieve data for.

        Returns:
            - pd.DataFrame: Pandas DataFrame with data for multiple stores.
        """

        try:
            # Initialise a list to store data for each store.
            stores_data = []

            # Initialise a list to store store numbers for which data retrieval failed.
            failed_stores = []  

             # Iterate over the specified number of stores.
            for store_number in range(0, number_stores):
                stores_url = f"{store_details_endpoint}{store_number}"
            
                # Send a GET request to the API endpoint for store details.
                response = requests.get(stores_url, headers=self.api_config['headers'])
                try:
                    # Check if the response status code indicates a successful request (200 OK).
                    if response.status_code == 200:
                        # Parse the JSON data from the response and append it to the list of store data.
                        store_data = response.json()
                        stores_data.append(store_data)
                    else: 
                        # If the request was not successful, add the store number to the list of failed stores.
                        failed_stores.append(store_number)
                
                except Exception as e:
                    logging.error(f'Error in data_extraction method retrieve_stores_data, data retrieval: {e}')
        
            # If there are failed stores, log a warning with information about the failures.
            if failed_stores:
                logging.warning(f'failed to retrieve data for {len(failed_stores)} stores: {failed_stores}')

            # Create a Pandas DataFrame from the list of store data.
            df = pd.DataFrame(stores_data)

            # Save the DataFrame to a CSV file locally.
            df.to_csv('csv_files/stores_table.csv', index=False)

            return df
        
        except Exception as e:
            logging.error(f'Error in data_extraction method retrieve_stores_data: {e}')
            return None

   ############################     
    def extract_from_s3(self, s3_address):
        """
        Extracts data from an S3 bucket.

        Parameters:
        ----------
            - s3_address (string): The address for the S3 bucket in the format 's3://bucket_name/object_key'.

        Returns:
            - pd.DataFrame: Pandas DataFrame with data from the S3 bucket.
        """

        try: 
             # Extract bucket name and object key from the provided S3 address.
            bucket_name, object_key = s3_address.replace('s3://', '').split('/', 1)
            
            # Initialise an S3 client
            s3 = boto3.client('s3')
            
            # Download CSV file from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)

            # Convert the CSV content to a Pandas DataFrame
            df = pd.read_csv(response['Body'])

            # Save the Pandas DataFrame to a local CSV file.
            df.to_csv('csv_files/products_table.csv', index=False)

            return df
        
        except Exception as e:
            logging.error(f'Error in data_extraction method extract_from_s3: {e}')


    def extract_json_from_url(self, json_url):
        """
        Extracts data from a JSON file at the specified URL.

        Parameters:
            - json_url (string): The URL of the JSON file.

        Returns:
            - pd.DataFrame: Pandas DataFrame with data from the JSON file.
        """

        try:
            # Send a GET request to the specified JSON URL.
            response = requests.get(json_url)

            if response.status_code == 200:
                # If the response status code is 200 (OK), extract JSON content.
                json_content =response.json()

                # Convert the JSON content to a Pandas DataFrame.
                df =pd.DataFrame(json_content)

                # Save the Pandas DataFrame to a local CSV file.
                df.to_csv('csv_files/date_times_table.csv', index=False)
                
                return df
            
            else: 
                logging.error(f'Error in data_extraction method extract_json_from_pdf. Status code: {response.status_code}')
                return None
        
        except Exception as e:
            logging.error(f'Error in data_extraction method extract_json_from_url: {e}')
            return None

# Main Execution        
if __name__ == "__main__":
    # Instantiate the DatabaseConnector
    db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file)

# The script ends here


