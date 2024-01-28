# Multinational Retail Data Centralisation 

# Acknowledgements
Special thanks to my AICore support engineers A., B., H., I., I., J., K., M., M., V., W., for their invaluable assistance! This journey wouldn't be as epic without their guidance.

# Overview
Welcome to the hub of retail data centralisation! 

In this project, I embark on building a complete data solution for a multinational organisation, covering every step from data acquisition to analysis. The project involves writing Python code to extract large datasets from multiple external sources and utilising the power of Pandas for data cleaning and analysis in Visual Code Studio.

Join me as I navigate the realms of PDFs, AWS buckets and databases, turning data chaos into structured insights, all powered by PostgreSQL.

# Key Steps in Visual Studio Code
- Python: Use object-oriented_programming to define classes, encapsulate methods avoiding circular imports with clear docstrings and comments
- Data Extraction: Extract large datasets from diverse data sources such ###.
- Data Cleaning and Analysis with Pandas: Leverage the capabilities of Pandas for cleaning and analysing the extracted data.
- Dataset Uploading: use cleaned datasets to populate database on a local PostgreSQL server

# Key Steps in Security 
- Avoid hardcoding credentials: define credentials through yaml import to esafetly extract sensitive information when establishing connection to external and local AWS RDS, s3 buckets, POstgresSQL database.
- use of SQLAlchemy engines?
- Gitignore: ensuring credentials are listed in .gitignore file to prevent unauthorised access when sharing project.
  
# Key Steps in business analysis in PGAdmin 
- Ensureing compatible datatype to enable cross-refernecing, joining tables
- Database Schema Design: Build a STAR-based database schema for optimised data storage and access.
- Complex SQL Queries: Perform complex SQL data queries to extract business insights and make informed decisions for the organisation. This includes aggregate function, left joining, CTE and window functions such as LAG.

# Sample code for runnign queries in Jupyter Notebook
I also provided an example code on extracting credentials securely to run SQL queries on POstgreSQL database from Visual Studio Code.  

## File Structure of the Project
main.py
database_utils.py
data_extraction.py
data_cleaning.py
db_creds.yaml

# Installation Instructions

# Python Version
Make sure you have a compatible Python version; this project is designed for Python 3.12.0. Check your Python version using:
If you don't have Python installed or need to upgrade, download the latest version from python.org.

# Required Modules
Make sure you have the required Python modules installed. You can install them using the following command:
pip install pandas psychorg tabula requests

Adjust the version of python in the command based on your Python version.

Usage Instructions
To check this  project, run the main.py file using the following command:
python main.py

Ensure you have the required Python version and modules installed before running the script.

## License Information
* MIT

