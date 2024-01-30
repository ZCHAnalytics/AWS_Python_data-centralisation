"""
File: main.py
Purpose: Orchestrating the data symphony, where classes converge, insights unfold, and knowledge is mastered.
Author: Zulfia
Date: January 2024

# A bid shoutout to the data pioneers: A., B., H., I., I., J., K., M., M., V., W., conductors of wisdom!
"""


# External Libraries
import logging

# Internal Libraries and Credentials
from database_utils import DatabaseConnector, aws_credentials_file, local_credentials_file
from data_extraction import DataExtractor, pdf_url, json_url, s3_address
from data_cleaning import DataCleaning

# Logging Configuration
logging.basicConfig(level=logging.INFO)


# Class Definition and Methods 
def initialise_classes(aws_credentials_file, local_credentials_file):
    """
    Initialises instances of DatabaseConnector, DataExtractor, and DataCleaning classes.

    Parameters:
    ----------
        - aws_credentials_file (str): File path for AWS credentials.
        - local_credentials_file (str): File path for local credentials.

    Returns:
    --------
        - DatabaseConnector: Instance of DatabaseConnector class.
        - DataExtractor: Instance of DataExtractor class.
        - DataCleaning: Instance of DataCleaning class.
    """
    try:
        db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file)
        data_extractor = DataExtractor(db_connector)
        data_cleaner = DataCleaning(data_extractor)
        return db_connector, data_extractor, data_cleaner
    
    except Exception as e:
        logging.error(f'Error in main method initialise_classes: {e}')


def etl_of_users_data(db_connector, data_cleaner):
    """
    Extracts, transforms, and loads users data from 'legacy_users'.

    Parameters:
    ----------
        - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
        - data_cleaner (DataCleaning): Instance of DataCleaning class.
    """
    try: 
        df = data_cleaner.clean_user_data('legacy_users')
        db_connector.upload_to_db(df, 'dim_users')       
    
    except Exception as e:
        logging.error(f'Error in main method etl_of_users_data: {e}')
    

def etl_of_cards_data(db_connector, data_cleaner, pdf_url):
    """
    Extracts, transforms, and loads cards data.

    Parameters:
    ----------
        - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
        - data_cleaner (DataCleaning): Instance of DataCleaning class.
        - pdf_url (str): URL of the PDF containing card data.
    """
    try:
        df = data_cleaner.clean_card_data(pdf_url)
        db_connector.upload_to_db(df, 'dim_card_details')

    except Exception as e:
        logging.error(f'Error in main method etl_of_cards_data: {e}')

def etl_of_stores_data(db_connector, data_extractor, data_cleaner):
    """
    Extracts, transforms, and loads stores data.

    Parameters:
    ----------
        - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
        - data_extractor (DataExtractor): Instance of DataExtractor class.
        - data_cleaner (DataCleaning): Instance of DataCleaning class.
    """
    try: 
        number_of_stores = data_extractor.list_number_of_stores()
        print(f'number of stores is {number_of_stores}')
        store_details_endpoint = data_extractor.api_config['store_details_endpoint']
        df_stores = data_extractor.retrieve_stores_data(store_details_endpoint, number_of_stores)
        df_stores = data_cleaner.clean_store_data(df_stores)
        db_connector.upload_to_db(df_stores, 'dim_store_details')

    except Exception as e:
        logging.error(f'Error in main method etl_of_stores_data: {e}')

def etl_of_products_data(db_connector, data_extractor, data_cleaner, s3_address):
    """
    Extracts, transforms, and loads products data.

    Parameters:
    ----------
        - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
        - data_extractor (DataExtractor): Instance of DataExtractor class.
        - data_cleaner (DataCleaning): Instance of DataCleaning class.
        - s3_address (str): S3 address containing products data.
    """

    try: 
        df_products = data_extractor.extract_from_s3(s3_address)
        df_products = data_cleaner.convert_product_weights(df_products)
        df_products = data_cleaner.clean_product_data(df_products)
        db_connector.upload_to_db(df_products, 'dim_products')

    except Exception as e:
        logging.error(f'Error in main method etl_of_products_data: {e}')


def etl_of_orders_data(db_connector, data_cleaner):
    """
    Extracts, transforms, and loads orders data.

    Parameters:
    ----------
        - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
        - data_cleaner (DataCleaning): Instance of DataCleaning class.
    """
    try:
        db_connector.list_db_tables()
        df_orders = data_cleaner.clean_orders_data('orders_table')
        db_connector.upload_to_db(df_orders, 'orders_table')

    except Exception as e:
        logging.error(f'Error in main.py, etl_of_orders_data method: {e}')
 
def etl_of_datetimes_data(db_connector, data_extractor, data_cleaner, json_url):
    try:
        df_dates = data_extractor.extract_json_from_url(json_url)
        df_dates = data_cleaner.clean_dates(df_dates)
        db_connector.upload_to_db(df_dates, 'dim_date_times')

    except Exception as e:
        logging.error(f'Error in main method etl_of_datetimes_data: {e}')
        
def main():
    try:
        # Call initialise_classes with credentials and configurations
        db_connector, data_extractor, data_cleaner = initialise_classes(aws_credentials_file, local_credentials_file)
        
    except Exception as e:
        logging.error(f'Error in main function initialisation: {e}')

    try:
        etl_of_users_data(db_connector, data_cleaner)
        etl_of_cards_data(db_connector, data_cleaner, pdf_url)
        etl_of_stores_data(db_connector, data_extractor, data_cleaner)
        etl_of_products_data(db_connector, data_extractor, data_cleaner, s3_address)    
        etl_of_orders_data(db_connector, data_cleaner)
        etl_of_datetimes_data(db_connector, data_extractor, data_cleaner, json_url)

    except Exception as e:
        logging.error(f'Error in main function ETL methods: {e}')


# Main Execution 
if __name__ == "__main__":
    main()

print('\n   Until the next data expedition, happy coding!')

# The script ends here
