#data_cleaning 
import pandas as pd
from dateutil import parser
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from sqlalchemy import text
import tabula
import re

class  DataCleaning: #Task2
    def __init__(self, extractor): #T2 
        self.extractor = extractor #T3 S6 
    
    #T3 S6 Create a method which will perform the cleaning of the user data.
    # clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
    def clean_user_data(self, table_name):
        users_table = self.extractor.read_rds_table(table_name)
        #print('\n'"Summary before cleaning:" '\n'users_table.info())
        users_table.replace("NULL", pd.NA, inplace=True) # Replace "NULL" with NaN
        users_table['first_name'] = users_table['first_name'].where(~users_table['first_name'].astype(str).str.contains(r'\d'), pd.NA)
        users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], format='mixed', errors='coerce')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], format='mixed', errors='coerce')
        users_table = users_table.dropna() #removing null values
        #people names has no numbers
        rows_with_numbers_in_first_name = users_table[users_table['first_name'].astype(str).str.contains(r'\d')]
        # editing country_codes
        users_table['country_code'].replace("GGB", 'GB', inplace=True) # Replace "GGB" with "GB"N
        #print("Summary after cleaning:")
        #print(users_table.info())
        return users_table
    
    def clean_card_data(self, combined_data):
        #print('card data summary before cleaning')
        #print(combined_data.info())
        combined_data.replace("NULL", pd.NA, inplace=True) # Replace "NULL" with Not available 
        combined_data = combined_data.dropna()
        combined_data.loc[:, 'card_number'] = combined_data['card_number'].astype(str)
        rows_with_letters = combined_data['card_number'].str.contains(r'[a-zA-Z]')
        combined_data = combined_data[~rows_with_letters] #removing 13 rows where values are mixture of letters and numbers
        #replacing ? with empty string
        combined_data['card_number'] = combined_data['card_number'].astype(str).apply(lambda x: x.lstrip('?'))
        #print('card data summary after cleaning')
        #print(combined_data.info())
        return combined_data
    
    def clean_store_data(self, extracted_store_data):
        cleaned_store_data = extracted_store_data.drop('lat', axis=1)
        rows_country_code_less_than_or_equal_to_3 = cleaned_store_data['country_code'].str.len() <= 3
        cleaned_store_data = cleaned_store_data[rows_country_code_less_than_or_equal_to_3].copy()  # Use .copy() ? 
        cleaned_store_data['opening_date'] = cleaned_store_data.loc['opening_date'] = pd.to_datetime(cleaned_store_data['opening_date'], format='mixed', errors='coerce')
        return cleaned_store_data

    def convert_product_weights(self, df, column_name='weight'):
        try:
            if column_name in df.columns:
                # conversion function
                def convert_weight_string(weight_str):
                    matches = re.findall(r'(\d+(\.\d+)?)', str(weight_str))
                    if matches:
                        numeric_values = [float(match[0]) for match in matches]
                        if 'kg' in weight_str:
                            return numeric_values[0]
                        elif 'x' in weight_str and 'g' in weight_str:  # multiply
                            multiplier, unit_weight = numeric_values[:2]
                            return multiplier * unit_weight / 1000
                        elif 'ml' in weight_str:
                            return numeric_values[0] / 1000
                        elif 'oz' in weight_str:
                            return numeric_values[0] * 28.35 / 1000
                        elif 'g' in weight_str:
                            return numeric_values[0] / 1000

                # call function on weight column
                df[column_name] = df[column_name].apply(convert_weight_string)

        except ValueError as error:
            print(f"Error during conversion: {error}")

    def clean_product_data(self, df):
        if df is None or df.empty:
            print("No valid data to clean.")
            return pd.DataFrame()      
        try:
            # null/NaN values
            null_values = df.isna().sum()
            print("Count of NaN values for each column:")
            print(null_values)

            # drop NaN values
            df = df.dropna()

            # drop leading and trailing spaces in 'product_price'
            df['product_price'] = df['product_price'].str.strip()

            # rows in 'product_price' with more than 5 characters
            price_more_than_7_chars = df['product_price'].str.len() > 7
            df = df[df['product_price'].str.len() <= 7]

            # 'date_added' to datetime format
            df['date_added'] = pd.to_datetime(df['date_added'], format='mixed', errors='coerce')
            null_values = df.isna().sum()
            # check NaN values per columns
            print("Count of NaN values for each column:")
            print(null_values)
            return df

        except Exception as e:
            print(f"Error during data cleaning: {e}")
            return pd.DataFrame()

    def clean_orders_data(self, table_name): 
        orders_table = self.extractor.read_rds_table(table_name)
        columns_to_remove = ['first_name', 'last_name', '1']
        orders_table = orders_table.drop(columns=columns_to_remove)
        return orders_table
    
    def clean_dates(self, dates): 
        print(dates.info())
        #dates.replace("NULL", pd.NA, inplace=True) # Replace "NULL" with NaN
        #dates['month'] = pd.to_datetime(dates['month'], format='mixed', errors='coerce').dt.month
        dates['year'] = pd.to_datetime(dates['year'], format='%Y', errors='coerce').dt.year
        dates['day'] = pd.to_datetime(dates['day'], format='%d', errors='coerce').dt.day
        return dates
        # rows with failed conversions
        failed_rows = dates[dates[['year', 'day']].isna().any(axis=1)]
        dates = dates.dropna(subset=['year', 'day'])
    
"""

user_data_cleaned = data_cleaner.clean_user_data('legacy_users')
extracted_card_data = data_extractorextractor.retrieve_pdf_data(pdf_url)
cleaned_card_data = data_cleaner.clean_card_data(extracted_card_data)

db_connector = DatabaseConnector()
data_extractor = DataExtractor(db_connector)
data_cleaner = DataCleaning(data_extractor)


# Extract data
s3_address = 's3://data-handling-public/products.csv'
extracted_products = data_extractor.extract_from_s3(s3_address)
df_s3 = pd.DataFrame(extracted_products)
data_cleaner.convert_product_weights(df_s3)
cleaned_data = data_cleaner.clean_product_data(df_s3)

pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
card_data = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
data_cleaner = DataCleaning(data_extractor)

user_data_cleaned = data_cleaner.clean_user_data('legacy_users')
extracted_card_data = data_extractorextractor.retrieve_pdf_data(pdf_url)
cleaned_card_data = data_cleaner.clean_card_data(extracted_card_data)

number_of_stores = data_extractorextractor.list_number_of_stores()
extracted_store_data = data_extractor.retrieve_stores_data(data_extractor.api_config['store_details_endpoint'], number_of_stores)
cleaned_store_data = data_cleaner.clean_store_data(extracted_store_data)
"""