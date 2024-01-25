MultinationL Retail Data Centralisation 

Table of Contents, if the README file is long
The aimd of the project is to extract data from different sources such as pdf files, AWS buckets in different formats; convert data into csv format; clean the data and load data in separate tables in postgres file. 
During this project, I learned how to write code to extract data from {} using [method], clean data to remove missing values, convert dates in different formats, converts weights provided in different formats, removed columns, 
standardised coutnry codes. I learned how to load data in local fodlers for analysis usign Excel Viewer in Visual COde Studio. 
I learned how to use  class methods to extract and upload data withtout hardcoding the access credentials.  

Installation instructions

Usage instructions
You need to use python file main.py. Make sure you have specific python 3.12.0, and relevant modules are: pandas, psychorg, dateutil, tabula (for pdf), requests (for API), re (for ..) 

File structure of the project
main:
database_utils.py
data_extraction.py
data_cleaning.py
db_creds.yaml
sales_data.session.sql

License information: MIT
