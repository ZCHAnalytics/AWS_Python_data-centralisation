#database_utils.pu
import yaml #Step 2
from sqlalchemy import create_engine, inspect # Step 3, 
import psycopg2

class DatabaseConnector: 
    def __init__(self, aws_credentials_file="db_creds.yaml", local_credentials_file="project_creds_local.yaml"):
        self.creds1, self.creds2 = self.read_db_creds(aws_credentials_file, local_credentials_file)
        self.engine1, self.engine2 = self.init_db_engine()
    # Step 2
    def read_db_creds(self, aws_credentials_file, local_credentials_file):
        with open(aws_credentials_file, 'r') as aws_file, open (local_credentials_file, 'r') as local_file:
            data1 = yaml.safe_load(aws_file)
            data2 = yaml.safe_load(local_file)
            return data1, data2
    # Step 3
    def init_db_engine(self):
        engine1 = create_engine(f"postgresql://{self.creds1['RDS_USER']}:{self.creds1['RDS_PASSWORD']}@{self.creds1['RDS_HOST']}:{self.creds1['RDS_PORT']}/{self.creds1['RDS_DATABASE']}")
        engine2 = create_engine(f"postgresql://{self.creds2['USER']}:{self.creds2['PASSWORD']}@{self.creds2['HOST']}:{self.creds2['PORT']}/{self.creds2['DATABASE']}")
        return engine1, engine2
    # Step 4
    def list_db_tables(self):
        inspector = inspect(self.engine1)
        return inspector.get_table_names()
    
    def upload_to_db(self, data_frame, table_name):
        data_frame.to_sql(table_name, con=self.engine2, if_exists='replace', index=False)

db_connector = DatabaseConnector()

#ETL  legacy_users table
#user_data = db_connector.list_db_tables()[0]
#cleaned_user_data = data_cleaner.clean_user_data('legacy_users')
#db_connector.upload_to_db(cleaned_user_data, 'dim_users')
#print('i tried to upload legacy users data')
