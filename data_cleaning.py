#data_cleaning
import logging 
import pandas as pd
import re
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from dateutil import parser

logging.basicConfig(level=logging.INFO)

class  DataCleaning: #Task2
    """
    Class for cleaning downloaded data to upload to postgres.

    Parameters:
    - extractor (DataExtractor): An instance of the DataExtractor class for extractign data from various sources.

    Methods:
    - clean_user_data(table_name): Cleans user data from a specific table.
    - clean_card_data(pdf_url): Cleans card data extracted from a PDF.
    - clean_store_data(extracted_store_data): Cleans stores data from the extracted stores data.
    - convert_product_weights(df): Converts product weights to a consistent format in kilos.
    - clean_product_data(df): Cleans product data after weights conversion.
    - clean_orders_data(table_name): Cleans orders data from a specific table.
    - clean_dates(df): Cleans date events data.
    """

    def __init__(self, extractor): #T2 
        """
        Initialises the DataCleaning instance.

        Parameters:
        - extractor (DataExtractor): An instance of the DataExtractor class for data extraction.
        """
          
        self.extractor = extractor #T3 S6 
        
    def clean_user_data(self, table_name):
        """
        Cleans user data from a specific table.

        Parameters:
        - table_name (str): The name of the required table.

        Returns:
        - pd.DataFrame: Cleaned user data.
        """
        try:    
            df = self.extractor.read_rds_table(table_name)
            df = df.drop('index', axis=1)
            df.replace("NULL", pd.NA, inplace=True) # Replace "NULL" with NaN
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='mixed', errors='coerce')
            df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce')
            df = df.dropna() #removing null values
            df['country_code'].replace("GGB", 'GB', inplace=True) # Replace "GGB" with "GB"N
            return df
        except Exception as e:
            logging.error(f'          CLEAN -- 2.4 clean_user_data -- cleaning failed: {e}')
            return None
    
    def clean_card_data(self, pdf_url):
        """
        Cleans card data extracted from a PDF.

        Parameters:
        - pdf_url (str): The URL of the PDF with card data.

        Returns:
        - pd.DataFrame: Cleaned card data.
        """
        combined_data = self.extractor.retrieve_pdf_data(pdf_url)
        # print('\n ****card data summary before cleaning')
        print(combined_data.info())
        try:
            combined_data.replace("NULL", pd.NA, inplace=True) # Replace "NULL" with Not available 
            combined_data = combined_data.dropna()
            combined_data.loc[:, 'card_number'] = combined_data['card_number'].astype(str)
            rows_with_letters = combined_data['card_number'].str.contains(r'[a-zA-Z]')
            combined_data = combined_data[~rows_with_letters] 
            # print('\n Card data summary after cleaning')
            print(combined_data.info())
            combined_data['card_number'] = combined_data['card_number'].astype(str).apply(lambda x: x.lstrip('?'))
            return combined_data
        except Exception as e:
            logging.error(f'          CLEAN -- 3.1 clean_card_data -- mixeds strings removal failed: {e}')
       
    def clean_store_data(self, extracted_store_data):
        """
        Cleans store data from the appended store tables.

        Parameters:
        - extracted_store_data (pd.DataFrame): Extracted store data.

        Returns:
        - pd.DataFrame: Cleaned store data.
        """
        try:
            df = extracted_store_data  #
            logging.info('df renamed')
            if not df.empty:  # Check if the DataFrame is not empty
                columns_to_drop = ['index', 'lat']
                df.drop(columns=columns_to_drop, inplace=True)
                logging.info('dropping index and lat columns')
                df.loc[0] = df.loc[0].replace('NULL', 'n/a')
                logging.info('replacing NULL values in webportal row with n/a values')
                # Filter rows where the length of 'country_code' is less than or equal to 3
                country_code_less_than_or_equal_to_3 = df['country_code'].str.len() <= 3
                df = df[country_code_less_than_or_equal_to_3]
                df.loc[:, 'opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
                df['continent'].replace({'eeAmerica': 'America', 'eeEurope': 'Europe'}, inplace=True)
                logging.info('Continent names corrected')
                # Extract only numeric characters from 'staff_numbers'
                df.loc[:, 'staff_numbers'] = df['staff_numbers'].str.extract(r'(\d+)')
                logging.info('Numeric characters extracted from staff_numbers')
                return df
            else:
                logging.warning('DataFrame is empty. No cleaning operations performed.')
                return df
        except Exception as e:
            logging.error(f'    data_cleaning.py -- 4 failed to clean stores table: {e}')

    def convert_product_weights(self, df):
        """
        Converts product weights to kilograms.

        Parameters:
        - df (pd.DataFrame): DataFrame containing product data.

        Returns:
        - pd.DataFrame: DataFrame with weights converted to kilograms.
        """
        try:
            def convert_weight_string(weight_str):
                try:
                    numeric_value = float(weight_str)
                    return numeric_value
                except ValueError:
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
            df['weight'] = df['weight'].apply(convert_weight_string)
            df = df.dropna(subset=['weight'])                
            return df
        except ValueError as e:
            logging.error(f'          CLEAN -- 5.2 convert_product_weights -- failure: {e}')
    
    def clean_product_data(self, df):
        """
        Cleans product data after weights conversion.

        Parameters:
        - df (pd.DataFrame): DataFrame containing product data.

        Returns:
        - pd.DataFrame: Cleaned product data ready for uploading.
        """
        columns_to_drop = [0, 1, 2, 3] 
        df = df.drop(df.columns[columns_to_drop], axis=1) 
        try:
            df['date_added'] = pd.to_datetime(df['date_added'], format='mixed', errors='coerce')
            return df
        except Exception as e:
            logging.error(f'          CLEAN -- 6.3 clean_product_data -- date format editing failed: {e}')
        return df
    def clean_orders_data(self, table_name): 
        """
        Cleans orders data from a specific table.

        Parameters:
        - table_name (str): The name of required tablethat has orders data.

        Returns:
        - pd.DataFrame: Cleaned orders data.
        """
        logging.info(f'table listed as {table_name}')
        try:
            orders_table = self.extractor.read_rds_table(table_name)
            logging.info('  data_cleaning.py -- extraction done')
            if orders_table.empty:
                logging.warning('   data_cleaning.py -- clean_orders_data -- no orders data retrived')
                return None
            columns_to_drop = ['level_0', 'first_name', 'last_name', '1']
            orders_table.drop(columns=columns_to_drop, axis=1)
            orders_table.drop(orders_table.columns[0], axis=1)
            # print('\n Orders Table after cleaning')
            print(orders_table.info())
            return orders_table
        except Exception as e:
            logging.info(f'cleaning orders data went wrong')

    def clean_dates(self, df): 
        """
        Cleans date time events data in the DataFrame.

        Parameters:
        - df (pd.DataFrame): DataFrame containing date time events data.

        Returns:
        - pd.DataFrame: Cleaned DataFrame.
        """
        # if the first column has unique values
        is_first_column_unique = df[df.columns[0]].is_unique
        # removing old index
        try:
            df = df.drop(df.columns[0], axis=1)
            month_over_3_characters = df['month'].str.len() > 4
            df = df[~month_over_3_characters]
            unique_months = df['month'].unique()

            # Replace string representation of NaN with actual NaN
            df.replace('NULL', pd.NA, inplace=True)
            df=df.dropna()
            unique_months = df['month'].unique()
            # print('\n unique characters after dropping NULL values')
            # print(unique_months)

            # boolean mask for rows with NaN values
            nan_rows_mask = df.isna().any(axis=1)
            # rows with NaN values
            df_nan_rows = df[nan_rows_mask]
            # print('\n df row with nan values')
            # print(df_nan_rows)
            df['year'] = pd.to_datetime(df['year'], format='%Y', errors='coerce').dt.year
            df['day'] = pd.to_datetime(df['day'], format='%d', errors='coerce').dt.day
            # print('\n final df')
            # print(df.info())
            return df
        except Exception as e:
            logging.error(f' Cleaning dates failed: {e}')