# main.py
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import pandas as pd
def main():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    data_cleaner = DataCleaning(data_extractor)
    
    #ETL Date Events
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    extracted_dates = data_extractor.extract_json_from_url(json_url)
    #df_s3 = pd.DataFrame(extracted_dates)
    cleaned_events_dates = data_cleaner.clean_dates(extracted_dates)
    print('cleaning done')
    
    #db_connector.upload_to_db(cleaned_events_dates, 'dim_date_times')

    """
    #ETL orders_table
    orders_table = db_connector.list_db_tables()
    cleaned_orders_table = data_cleaner.clean_orders_data('orders_table')
    db_connector.upload_to_db(cleaned_orders_table, 'orders_table')
   
     #ETL  legacy_users table
    user_data = db_connector.list_db_tables()
    cleaned_user_data = data_cleaner.clean_user_data('legacy_users')
    db_connector.upload_to_db(cleaned_user_data, 'dim_users')
    print('i tried to upload legacy users data')

    #ETL card_data
    pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    extracted_card_data = data_extractor.retrieve_pdf_data(pdf_url)
    clean_card_data = data_cleaner.clean_card_data(extracted_card_data)
    db_connector.upload_to_db(clean_card_data, 'dim_card_details')
    print('i tried uploading card data')
    
    #ETL stores data
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    number_of_stores = data_extractor.list_number_of_stores()
    store_details_endpoint = data_extractor.api_config['store_details_endpoint']
    extracted_store_data = data_extractor.retrieve_stores_data(store_details_endpoint, number_of_stores)

    cleaned_store_data = data_cleaner.clean_store_data(extracted_store_data)
    db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')
    print('yep, i tried uploading store dataframe')
    
    #ETL product data
    s3_address = 's3://data-handling-public/products.csv'
    extracted_products = data_extractor.extract_from_s3(s3_address)
    df_s3 = pd.DataFrame(extracted_products)
    data_cleaner.convert_product_weights(df_s3)
    cleaned_products_data = data_cleaner.clean_product_data(df_s3)
    db_connector.upload_to_db(cleaned_products_data, 'dim_products')
    """
if __name__ == "__main__":
    main()
