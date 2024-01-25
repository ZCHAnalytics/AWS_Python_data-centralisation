from sqlalchemy import create_engine, inspect # Step 3, 
import logging
import yaml #Step 2

logging.basicConfig(level=logging.INFO)

aws_credentials_file="db_creds.yaml"
with open('api_config.yaml', 'r') as config_file:
    config_data = yaml.safe_load(config_file)
api_config = {
    'headers': {'x-api-key': config_data['api_key']},
    'number_of_stores_endpoint': config_data['number_of_stores_endpoint'],
    'store_details_endpoint': config_data['store_details_endpoint']
}
local_credentials_file="project_creds_local.yaml"

class DatabaseConnector: 

    """
    Class for initiating connections for accessing external sources and uploading datasets to postgres.

    Attributes:
    - aws_credentials_file (str): File path to the AWS credentials file.
    - local_credentials_file (str): File path to the local credentials file.
    - api_config (dict): Configuration settings for API connections.
    - creds1 (dict): Database credentials for the first engine.
    - creds2 (dict): Database credentials for the second engine.
    - engine1 (sqlalchemy.engine.Engine): SQLAlchemy engine for the first database.
    - engine2 (sqlalchemy.engine.Engine): SQLAlchemy engine for the second database.
    """

    def __init__(self, aws_credentials_file, local_credentials_file, api_config):
        
        """
        Initialises the DatabaseConnector instance.

        Parameters:
        - aws_credentials_file (str): File path to the AWS credentials file.
        - local_credentials_file (str): File path to the local credentials file.
        - api_config (dict): Configuration settings for API connections.
        """
        
        self.aws_credentials_file = aws_credentials_file
        self.local_credentials_file = local_credentials_file
        self.api_config = self.read_api_config()
        try: 
            self.creds1, self.creds2 = self.read_db_creds(aws_credentials_file, local_credentials_file)
        except Exception as e:
            logging.error(f'    database_utils.py -- 0.1 init --  failed: {e}')
        try:    
            self.engine1 = self.init_db_engine1()
        except Exception as e:
            logging.error(f'    database_utils.py -- 0.2 init -- db_engine 1 failed: {e}')
        try:
            self.engine2 = self.init_db_engine2()
        except Exception as e:
            logging.error(f'    database_utils.py -- 0.3 init -- db_engine 2 failed: {e}')
    
    def read_api_config(self):
        """
        Obtains API configuration from the separate file.

        Returns:
        - dict: Configuration settings for API connections.
        """

        try:
            with open('api_config.yaml', 'r') as config_file:
                config_data = yaml.safe_load(config_file)
            api_config = {
                'headers': {'x-api-key': config_data['api_key']},
                'number_of_stores_endpoint': config_data['number_of_stores_endpoint'],
                'store_details_endpoint': config_data['store_details_endpoint']
            }
            return api_config
        except Exception as e:
            raise ValueError(f' database_utils.py -- cError reading API config file: {e}')
    # Step 2
    def read_db_creds(self, aws_credentials_file, local_credentials_file):
        
        """
        Reads aws credentials from a separate file.

        Parameters:
        - aws_credentials_file (str) ???? AWS credentials file.
        - local_credentials_file (str): ???  local credentials file.

        Returns:
        - Tuple: Database credentials for the first and second engines.
        """

        try:
            with open(aws_credentials_file, 'r') as aws_file, open (local_credentials_file, 'r') as local_file:
                data1 = yaml.safe_load(aws_file)
                data2 = yaml.safe_load(local_file)
            return data1, data2
        except Exception as e:
            logging.error(f'    database_utils.py -- 1 read_db_creds() -- yaml AWS access failed: {e}')
        
    def init_db_engine1(self):
        """
        Initialises the SQLAlchemy engine for downloading files.

        Returns:
        - sqlalchemy.engine.Engine: SQLAlchemy engine for downloading files.
        """
        try:
            engine1 = create_engine(f"postgresql://{self.creds1['RDS_USER']}:{self.creds1['RDS_PASSWORD']}@{self.creds1['RDS_HOST']}:{self.creds1['RDS_PORT']}/{self.creds1['RDS_DATABASE']}")
            return engine1
        except Exception as e:
            logging.error(f'    database_utils.py -- 2 init_db_engine1() failed: {e}')

    def init_db_engine2(self):    
        """
        Initialises the SQLAlchemy engine for uploading files to postgres.

        Returns:
        - sqlalchemy.engine.Engine: SQLAlchemy engine for uploading files to postgres.
        """
        try:
            engine2 = create_engine(f"postgresql://{self.creds2['USER']}:{self.creds2['PASSWORD']}@{self.creds2['HOST']}:{self.creds2['PORT']}/{self.creds2['DATABASE']}")
            return engine2
        except Exception as e:
            logging.error(f'    database_utils.py -- 2 init_db_engine2() failed: {e}')
            
    def list_db_tables(self):
        """
        Creates a list of tables received from a source.

        Returns:
        - list: List of table names obtained from externnal source.
        """
        try: 
            inspector = inspect(self.engine1)
            return inspector.get_table_names()
        except Exception as e:
            logging.error(f'    database_utils.py -- 3 list_db_tables() -- failed: {e}')
    
    def upload_to_db(self, df, destination_table_name):
        """
        Uploads a DataFrame to postgres.

        Parameters:
        - df (pandas.DataFrame): The DataFrame to be uploaded.
        - destination_table_name (str): The name of the destination table in postgres.
        """
        try:
            if df is None:
                logging.warning(f'  database_utils.py -- 4 upload_to_db() -- Attempted to upload NoneType DataFrame to {destination_table_name}')
                return
            df.to_sql(name=destination_table_name, con=self.engine2, if_exists='replace', index=False)
        except Exception as e:
            logging.error(f'    database_utils.py -- 4 upload_to_db() -- {destination_table_name} upload failed: {e}')
    
if __name__ == "__main__":
    db_connector = DatabaseConnector(aws_credentials_file, local_credentials_file, api_config)
    db_connector.read_db_creds(aws_credentials_file, local_credentials_file)
    db_connector.init_db_engine1()
    db_connector.init_db_engine2()

