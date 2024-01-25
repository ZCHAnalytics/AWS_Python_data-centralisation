import logging
from database_utils import DatabaseConnector, aws_credentials_file, local_credentials_file, api_config
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

logging.basicConfig(level=logging.INFO)

# Links
s3_address = 's3://data-handling-public/products.csv' 
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

def initialise_classes(aws_credentials_file, local_credentials_file, api_config):
    """
    Initialises instances of DatabaseConnector, DataExtractor, and DataCleaning classes.

    Parameters:
    - aws_credentials_file (str): File path for AWS credentials.
    - local_credentials_file (str): File path for local credentials.
    - api_config (dict): Configuration for API.

    Returns:
    - DatabaseConnector: Instance of DatabaseConnector class.
    - DataExtractor: Instance of DataExtractor class.
    - DataCleaning: Instance of DataCleaning class.
    """
    try:
        db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file, api_config)
        data_extractor = DataExtractor(db_connector, api_config)
        data_cleaner = DataCleaning(data_extractor)
        return db_connector, data_extractor, data_cleaner
    except Exception as e:
        logging.error(f'    main.py -- 0.0 initialise_classes -- failed: {e}')

def etl_of_users_data(db_connector, data_cleaner):
    """
    Extracts, transforms, and loads users data from 'leagacy_users'.

    Parameters:
    - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
    - data_cleaner (DataCleaning): Instance of DataCleaning class.

    Returns:
    - pd.DataFrame: Cleaned users data.
    """
    try: 
        cleaned_users_table = data_cleaner.clean_user_data('legacy_users')
        db_connector.upload_to_db(cleaned_users_table, 'dim_users_test')       
    except Exception as upload_error:
            logging.error(f'    main.py -- 1 Users data ETL -- failure: {upload_error}')
    return cleaned_users_table

def etl_of_cards_data(db_connector, data_cleaner, pdf_url):
    """
    Extracts, transforms, and loads cards data.

    Parameters:
    - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
    - data_cleaner (DataCleaning): Instance of DataCleaning class.
    - pdf_url (str): URL of the PDF containing card data.
    """
    try:
        cleaned_card_df = data_cleaner.clean_card_data(pdf_url)
        db_connector.upload_to_db(cleaned_card_df, 'dim_card_details_test')
    except Exception as e:
        logging.error(f'    main.py 2 Cards data ETL --  failure: {e}')

def etl_of_stores_data(db_connector, data_extractor, data_cleaner):
    """
    Extracts, transforms, and loads stores data.

    Parameters:
    - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
    - data_extractor (DataExtractor): Instance of DataExtractor class.
    - data_cleaner (DataCleaning): Instance of DataCleaning class.

    Returns:
    - pd.DataFrame: Cleaned stores data.
    """
    try: 
        number_of_stores = data_extractor.list_number_of_stores()
        logging.info(f' main.py -- ETL stores number extracted: {number_of_stores}')
        store_details_endpoint = data_extractor.api_config['store_details_endpoint']
        logging.info('store api configured')
        extracted_store_data = data_extractor.retrieve_stores_data(store_details_endpoint, number_of_stores)
        logging.info('data extracted')
        cleaned_store_data = data_cleaner.clean_store_data(extracted_store_data)
        logging.info('cleaned')
        db_connector.upload_to_db(cleaned_store_data, 'dim_store_details_test')
    except Exception as e:
        logging.error(f'    main.py -- 3 Stores data ETL -- failure: {e}')


def etl_of_products_data(db_connector, data_extractor, data_cleaner, s3_address):
    """
    Extracts, transforms, and loads products data.

    Parameters:
    - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
    - data_extractor (DataExtractor): Instance of DataExtractor class.
    - data_cleaner (DataCleaning): Instance of DataCleaning class.
    - s3_address (str): S3 address containing products data.
    """

    try: 
        extracted_products = data_extractor.extract_from_s3(s3_address)
        converted_products_df = data_cleaner.convert_product_weights(extracted_products)
        cleaned_products_df = data_cleaner.clean_product_data(converted_products_df)
        db_connector.upload_to_db(cleaned_products_df, 'dim_products_test')
    except Exception as e:
        logging.error(f'    main.py -- 4 Products data ETL -- failure : {e}')

def etl_of_orders_data(db_connector, data_cleaner):
    """
    Extracts, transforms, and loads orders data.

    Parameters:
    - db_connector (DatabaseConnector): Instance of DatabaseConnector class.
    - data_cleaner (DataCleaning): Instance of DataCleaning class.
    """
    try: 
        list_of_tables = db_connector.list_db_tables()
        logging.info(f' main.py -- etl orders_data -- table name listed: {list_of_tables} ')
        cleaned_orders_df = data_cleaner.clean_orders_data('orders_table')
        logging.info('  main.py -- etl orders -- orders-table found')
        db_connector.upload_to_db(cleaned_orders_df, 'orders_table_test')

    except Exception as e:
        logging.error(f'    main.py -- 5 Orders data ETL -- failure: {e}')
 
def etl_of_datetimes_data(db_connector, data_extractor, data_cleaner, json_url):
    try:
        extracted_datetimes = data_extractor.extract_json_from_url(json_url)
        cleaned_datetimes = data_cleaner.clean_dates(extracted_datetimes) 
        db_connector.upload_to_db(cleaned_datetimes, 'dim_date_times_test')
    except Exception as e:
        logging.error(f'    main.py -- 6 Datetimes ETL -- failure: {e}')

def main():
    try:
        # Call initialise_classes with credentials and configurations
        db_connector, data_extractor, data_cleaner = initialise_classes(aws_credentials_file, local_credentials_file, api_config)
    except Exception as e:
        logging.error(f'    main.py -- classes failed to initialise: {e}')
        return  
    try:
        etl_of_users_data(db_connector, data_cleaner)
        etl_of_cards_data(db_connector, data_cleaner, pdf_url)
        #etl_of_stores_data(db_connector, data_extractor, data_cleaner)
        etl_of_products_data(db_connector, data_extractor, data_cleaner, s3_address)    
        etl_of_orders_data(db_connector, data_cleaner)
        etl_of_datetimes_data(db_connector, data_extractor, data_cleaner, json_url)

    except Exception as e:
        logging.error(f'    main.py -- main function failed to complete: {e}')
        return
    
if __name__ == "__main__":
    main()