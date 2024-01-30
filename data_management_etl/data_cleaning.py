"""
File: data_cleaning.py
Purpose: Polishing the data and bringing order to the chaos.
Author: Zulfia
Date: January 2024

# Special thanks to my AICore support engineers A., B., H., I., I., J., K., M., M., V., W., , for the invaluable assistance!
"""

# External Libraries
import logging 
import pandas as pd
import re

# Internal Libraries and Credentials
from database_utils import DatabaseConnector, aws_credentials_file, local_credentials_file
from data_extraction import DataExtractor

# Logging Configuration
logging.basicConfig(level=logging.INFO)

# Class definition and methods 
class DataCleaning:
    """
    Class for cleaning downloaded data.

    Parameters: 
        - extractor: An instance of the DataExtractor class for extracting data from various sources.

    Methods:
        - __init__(self, extractor): Initialises the DataCleaning instance.
        - clean_user_data(table_name): Cleans user data.
        - clean_card_data(pdf_url): Cleans card data.
        - clean_store_data(df): Cleans stores data.
        - convert_product_weights(df): Converts product weights to a consistent format in kilos.
        - clean_product_data(df): Cleans converted product data.
        - clean_orders_data(table_name): Cleans orders data.
        - clean_dates(df): Cleans date events data.
    """

    def __init__(self, extractor):
        """
        Initialises the DataCleaning instance.

        Parameter: 
            - extractor (DataExtractor): An instance of the DataExtractor class for data extraction.
        """
        self.extractor = extractor
        

    def clean_user_data(self, table_name):
        """
        Cleans user data from a specific table.

        Parameter: 
            - table_name (str): The name of the required table.

        Returns: 
            - A cleaned Pandas DataFrame ready for uploading or None if cleaning fails.
        """
        try:
            # Retrieve user data from the specified table
            df_users = self.extractor.read_rds_table(table_name)
                        
            if df_users.empty:
                logging.warning('Error in data_cleaning method clean_user_data, data retrieval')
                return None

            # Drop rendundant index column
            df_users = df_users.drop('index', axis=1)
            
            # Replace "NULL" string with NaN
            df_users.replace("NULL", pd.NA, inplace=True) 

            # Convert dates of birth into datetime format
            df_users['date_of_birth'] = pd.to_datetime(df_users['date_of_birth'], format='mixed', errors='coerce')

            # Convert joining dates into datetime format
            df_users['join_date'] = pd.to_datetime(df_users['join_date'], format='mixed', errors='coerce')

            # Remove NaN values 
            df_users = df_users.dropna()

            # Replace "GGB" with "GB"
            df_users['country_code'].replace("GGB", 'GB', inplace=True) 

            return df_users
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method clean_user_data: {e}')
            return None
    
    def clean_card_data(self, pdf_url):
        """
        Cleans card data extracted from a PDF.

        Parameters:
            - pdf_url (str): The URL of the PDF with card data.

        Returns:
            - A cleaned Pandas DataFrame ready for uploadin or None if cleaning fails.
        """
    
        try:
            # Retrieve card data from the specified PDF
            df_cards = self.extractor.retrieve_pdf_data(pdf_url)            
                        
            if df_cards.empty:
                logging.warning('Error in data_cleaning method clean_card_data, data retrieval')
                return None

            # Replace "NULL" string with NaN 
            df_cards.replace("NULL", pd.NA, inplace=True) 
            
            # Remove missing values 
            df_cards = df_cards.dropna()
            
            # Convert card number into strings and removing rows where they containg alphabetical characters (wrong pattern)
            df_cards.loc[:, 'card_number'] = df_cards['card_number'].astype(str)
            rows_with_letters = df_cards['card_number'].str.contains(r'[a-zA-Z]')
            df_cards = df_cards[~rows_with_letters] 
            
            # Edit card numbers to remove trailing character '?'
            df_cards['card_number'] = df_cards['card_number'].astype(str).apply(lambda x: x.lstrip('?'))

            return df_cards
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method clean_card_data: {e}')
        

    def clean_store_data(self, df_stores):
        """
        Cleans store data from the appended store tables.

        Parameters:
        - df_stores (pd.DataFrame): Extracted store data.

        Returns:
        - A cleaned Pandas DataFrame ready for uploading or None if cleaning fails.
        """
        try:
            logging.info('cleaning of stores started')
            # Check that the DataFrame is not empty
            if not df_stores.empty:  

                # Remove redundant columns 'index' and 'lat' 
                columns_to_drop = ['index', 'lat']
                df_stores.drop(columns=columns_to_drop, inplace=True)
                
                # Remove rows where 'country_code' is more than 3 characters
                country_code_less_than_or_equal_to_3 = df_stores['country_code'].str.len() <= 3
                df_stores = df_stores[country_code_less_than_or_equal_to_3]
                
                # Convert dates into datetime format
                df_stores.loc[:, 'opening_date'] = pd.to_datetime(df_stores['opening_date'], format='mixed', errors='coerce')
            
                # Replace incorrect continent names with the correct ones
                df_stores.loc[:, 'continent'] = df_stores['continent'].replace({'eeAmerica': 'America', 'eeEurope': 'Europe'})

                # Remove alphabetical characters from 'staff_numbers' to ensure correct format
                df_stores.loc[:, 'staff_numbers'] = df_stores['staff_numbers'].str.replace(r'\D', '', regex=True)

                return df_stores
            
            else:
                logging.warning('DataFrame is empty. No cleaning operations performed.')
            
        except Exception as e:
            logging.error(f'Error in data_cleaning method clean_store_data: {e}')
            return None

    def convert_product_weights(self, df_weights):
        """
        Converts product weights to kilograms.

        Parameters:
            - df_weights (pd.DataFrame): DataFrame containing product data.

        Returns:
            - A cleaned Pandas DataFrame with weights converted to kilograms or None if conversion fails.
        """
        try:
                        
            if df_weights.empty:
                logging.warning('Error data_cleaning method convert_product_weights method, data retrieval')
                return None
            
            # Inner function to convert a weight string to a numeric value
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
                return None
            
            # Apply the convert_weight_string function to the 'weight' column
            df_weights['weight'] = df_weights['weight'].apply(convert_weight_string)

            # Remove rows with NaN values in the 'weight' column
            df_weights = df_weights.dropna(subset=['weight'])                
            
            return df_weights
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method convert_product_weights: {e}')
            return None
        
    def clean_product_data(self, df_products):
        """
        Cleans product data after weights conversion.

        Parameter: 
            - df_products: a Pandas DataFrame called df containing product data.

        Returns:
            - A cleaned Pandas DataFrame ready for uploading or None if cleaning fails.
        """
        try: 
                        
            if df_products.empty:
                logging.warning('Error in data_cleaning method clean_product_data, data retrieval')
                return None

            # Remove redundant columns by their index
            df_products = df_products.drop(df_products.columns[0], axis=1)

            df_products.replace("NULL", pd.NA, inplace=True)

            # Count rows with all NaN values
            #df = df[df.isna().all(axis=1)].shape[0]

            # Drop rows with all NaN values
            df_products = df_products.dropna(how='all')
            
            # Remove leading and trailing spaces in 'product_price'
            df_products.loc[:, 'product_price'] = df_products['product_price'].str.strip()
            
            # Remove rows in 'product_price' with more than 5 characters (incorrect values)
            df_products = df_products[df_products['product_price'].str.len() <= 7]
            
            # Convert column 'date_added' column to datetime format
            df_products['date_added'] = pd.to_datetime(df_products['date_added'], format='mixed', errors='coerce')
            
            return df_products
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method clean_product_data: {e}')

    
    def clean_orders_data(self, table_name): 
        """
        Cleans orders data from a specific table.

        Parameters:
            - table_name (str): The name of required tablethat has orders data.

        Returns:
            - A cleaned Pandas pDataFrame ready for uploading or None if cleaning fails.
        """
         
        try:
            # Retrieve orders data from the specified table
            df_orders = self.extractor.read_rds_table(table_name)
            
            if df_orders.empty:
                logging.warning('Error in data_cleaning method clean_orders_data, data retrieval')
                return None
            
            # Remove redundant columns by their names, if available, or else by their index number
            columns_to_drop = ['level_0', 'first_name', 'last_name', '1']
            df_orders.drop(columns=columns_to_drop, axis=1, inplace=True)
            df_orders.drop(df_orders.columns[0], axis=1, inplace=True)

            return df_orders
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method clean_orders_data: {e}')
            return None


    def clean_dates(self, df_dates): 
        """
        Cleans date time events data in the DataFrame.

        Parameters: 
            - df_dates: a Pandas DataFrame called df containing date time events data.

        Returns: 
            - A cleaned Pandas DataFrame ready for uploading or None if cleaning fails.
        """
        try:
            if df_dates.empty:
                logging.warning('Error in data_cleaning method clean_dates, data retrieval')
                return None

            # Remove rows where month length is over 3 characters (incorrect values)
            month_over_3_characters = df_dates['month'].str.len() > 3
            df_dates = df_dates[~month_over_3_characters]
            
            # Convert timestamp, year and month columns into correct formats
            df_dates.loc[:, 'timestamp'] = pd.to_datetime(df_dates['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
            df_dates.loc[:, 'year'] = pd.to_datetime(df_dates['year'], format='%Y', errors='coerce').dt.year
            df_dates.loc[:, 'month'] = pd.to_datetime(df_dates['month'], format='%m', errors='coerce').dt.month
            
            return df_dates
        
        except Exception as e:
            logging.error(f'Error in data_cleaning method: {e}')
            return None
        
# Main Execution  
if __name__ == "__main__":
    # Instantiates the DatabaseConnector
    db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file)

    # Instantiates the DataExtractor
    extractor = DataExtractor(db_connector)

    # Instantiates the DataCleaning with the extractor
    data_cleaner = DataCleaning(extractor)
    
    print('\nAll sparkly clean! Data cleaning scripts breezed through without a speck of trouble!')

# The script ends here
