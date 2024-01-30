"""
File: database_utils.py
Purpose: A handy tool for connecting the dots in the database world.
Author: Zulfia 
Date: January 2024

# A shoutout to the data navigators: A., B., H., I., I., J., K., M., M., V., W., for their indispensable guidance and support!
"""

# External Libraries
import logging
import yaml
from sqlalchemy import create_engine, inspect

# Logging Configuration
logging.basicConfig(level=logging.INFO)


# Credentials File Paths (assuming they are in the same directory as this script)
aws_credentials_file="db_creds.yaml"
local_credentials_file="project_creds_local.yaml"


# DatabaseConnector Class and Methods 
class DatabaseConnector: 
    """
    Class for initiating connections for accessing external sources and uploading datasets to PostgreSQL local server.

    Attributes:
    ----------
        - aws_credentials_file (str): File path to the AWS credentials file.
        - local_credentials_file (str): File path to the local credentials file.
        - api_config (dict): Configuration settings for API connections.
        - creds1 (dict): Database credentials to initialise an engine for downloading data.
        - creds2 (dict): Database credentials to run an engine for uploading cleaned data.
        - engine1 (sqlalchemy.engine.Engine): SQLAlchemy engine for downloading.
        - engine2 (sqlalchemy.engine.Engine): SQLAlchemy engine for uploading.

    Methods:
    --------
        - __init__(self, aws_credentials_file, local_credentials_file, api_config): Initialises the DatabaseConnector instance.
        - def read_api_config(self): Obtains API configuration from a separate file.
        - def read_db_creds(self, aws_credentials_file, local_credentials_file): Reads AWS credentials from a separate file.
        - def init_db_engine_external(self): Initialises the SQLAlchemy engine to connect to external sources.
        - def init_db_engine_local(self): Initialises the SQLAlchemy engine to connect to local PostgreSQL database.
        - def list_db_tables(self): Creates a list of tables received from a source.
        - def upload_to_db(self, df, destination_table_name): Uploads dataframes to local PostgreSQL database.
    """
    
    def __init__(self, aws_credentials_file, local_credentials_file):
        """
        Initialises the DatabaseConnector instance.

        Parameters:
        ----------
            - aws_credentials_file (str): File path to the AWS credentials file.
            - local_credentials_file (str): File path to the local credentials file.
            - api_config (dict): Configuration settings for API connections.
        """
        
        # Assign input parameters to instance variables
        self.aws_credentials_file = aws_credentials_file
        self.local_credentials_file = local_credentials_file

        try:
            # Attempt to read credentials 
            self.creds1, self.creds2 = self.read_db_creds(aws_credentials_file, local_credentials_file)
        except Exception as e:
            logging.error(f'Error in database_utils.py, init method, reading credentials: {e}')
        
        try:
            # Attempt to initialise database engine for downloading    
            self.external_data_engine = self.init_db_engine_external()
        except Exception as e:
            logging.error(f'Error in database_utils.py, init method, external data engine: {e}')
        
        try:
            # Attempt to initialise database engine for uploading
            self.local_data_engine = self.init_db_engine_local()
        except Exception as e:
            logging.error(f'Error in database_utils.py, init method, local data engine: {e}')
    

    def read_api_config(self):
        """ Obtains API configuration from a separate file. Returns configuration settings for API connections."""

        try:
            # Open the 'api_config.yaml' file for reading
            with open('api_config.yaml', 'r') as config_file:
                 # Loading the content of the file using yaml.safe_load
                config_data = yaml.safe_load(config_file)
            
            # Create a dictionary with specific keys and values from the loaded data
            api_config = {
                'headers': {'x-api-key': config_data['api_key']},
                'number_of_stores_endpoint': config_data['number_of_stores_endpoint'],
                'store_details_endpoint': config_data['store_details_endpoint']
            }
            return api_config
        
        except Exception as e:
            logging.error(f'Error in database_utils method read_api_config: {e}')
    
    def read_db_creds(self, aws_credentials_file, local_credentials_file):
        """
        Reads AWS credentials from a separate file.

        Parameters:
        ----------
            - aws_credentials_file (str): File path to AWS credentials file.
            - local_credentials_file (str): File path to local credentials file.

        Returns:
            - Tuple: Database credentials for the first and second engines.
        """

        try:
            # Open the AWS credentials file and local credentials file
            with open(aws_credentials_file, 'r') as aws_file, open (local_credentials_file, 'r') as local_file:
                aws_credentials = yaml.safe_load(aws_file)
                local_db_credentials = yaml.safe_load(local_file)
            return aws_credentials, local_db_credentials
        
        except Exception as e:
            logging.error(f'Error in database_utils method read_db_creds: {e}')

    def init_db_engine_external(self):
        """
        Initialises the SQLAlchemy engine for downloading files.

        Returns:
            - sqlalchemy.engine.Engine: SQLAlchemy engine for downloading files.
        """
        
        try:
            # Construct a connection string for the external database
            external_data_engine = create_engine(f"postgresql://{self.creds1['RDS_USER']}:{self.creds1['RDS_PASSWORD']}@{self.creds1['RDS_HOST']}:{self.creds1['RDS_PORT']}/{self.creds1['RDS_DATABASE']}")
            return external_data_engine
        
        except Exception as e:
            logging.error(f'Error in database_utils method init_db_engine_external: {e}')

    def init_db_engine_local(self):    
        """
        Initialises the SQLAlchemy engine for uploading files to postgres.

        Returns:
            - sqlalchemy.engine.Engine: SQLAlchemy engine for uploading files to postgres.
        """

        try:
            # Construct a connection string to a local PostgreSQL database
            local_data_engine = create_engine(f"postgresql://{self.creds2['USER']}:{self.creds2['PASSWORD']}@{self.creds2['HOST']}:{self.creds2['PORT']}/{self.creds2['DATABASE']}")
            return local_data_engine
        
        except Exception as e:
            logging.error(f'Error in database_utils method init_db_engine_local: {e}')

    def list_db_tables(self):
        """
        Creates a list of tables received from a source.

        Returns:
            - list: List of table names obtained from externnal source.
        """

        try: 
             # Use the inspector to get a list of table names
            inspector = inspect(self.external_data_engine)
            return inspector.get_table_names()
        
        except Exception as e:
            logging.error(f'Error in database_utils method init_db_engine_external: {e}')

    def upload_to_db(self, df, destination_table_name):
        """
        Uploads Pandas DataFrames to PostgreSQL.

        Parameters:
        ----------
            - df (pandas.DataFrame): Dataframe to be uploaded.
            - destination_table_name (str): The name of the destination table in PostgreSQL database.
        """

        try:
            # Check there is a dataframe for uploading
            if df is None:
                logging.warning(f'Error in database_utils method upload_to_db, data processing')
                return
            
            # Use local_data_engine to upload cleaned dataframe to the specified destination table
            df.to_sql(name=destination_table_name, con=self.local_data_engine, if_exists='replace', index=False)
        
        except Exception as e:
            logging.error(f'Error in database_utils method upload_to_db: {e}')
    
# Main Execution    
if __name__ == "__main__":
    # Instantiating the DatabaseConnector
    db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file)

# The script ends here
