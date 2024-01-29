# Acknowledgements
Special thanks to my AICore support engineers A., B., H., I., I., J., K., M., M., V., W., for their invaluable assistance! This journey wouldn't be as epic without their guidance.

# Multinational Retail Data Centralisation

# Project Overview
Welcome to the hub of retail data centralisation! In this project, I embark on building a complete data solution for a multinational organisation, covering every step from setting up programming environment, data acquisition to analysis. The project involves writing scripts in Python programming language to extract large datasets from multiple external sources and utilising the power of Pandas for data cleaning in Visual Studio Code.

Join me as I navigate the realms of PDFs, AWS buckets, and databases, turning data chaos into structured insights, all powered by PostgreSQL.

# CONTENT
. Key Steps in Setting Connections
. Key Steps in Data Extraction
. Key Steps in Data Cleaning and Transformation with Pandas
. Key Steps in Dataset Uploading
. Key Steps in Security
. Key Steps in Creating Database Schema
. Key Steps in Business Analysis in PGAdmin
. Sample Business Queries
. File Structure of the Project
. Python 3.12.0 as the Programming Language
. Project Python Packages
. Project Environments

# Python 3.12.0 as the Programming Language:
The project leverages object-oriented programming (OOP) principles to enhance code organization and maintainability. 

# Project Python Packages:
- PyYAML: allows to work with YAML files, enabling the secure storage of credentials outside of hard code.
- SQLAlchemy: a SQL toolkit and Object-Relational Mapping (ORM) library for Python. It enabled secure connection to the AWS RDS database in the cloud for data extraction and to local database for uploading cleand data. 
- Pandas: a powerful data manipulation and analysis library that helped convert data structures into the form of DataFrame for efficient data manipulation with integrated indexing, weights, date conversion, removing unnecessary values.
- Boto3: the Amazon Web Services (AWS) Software Development Kit (SDK) for Python that helps extract project data from AWS S3 buckets.
- re (Regular Expressions): provides regular expression matching operations in Python. It was used for pattern matching with strings and allows  to search, match, and manipulate values based on patterns, such as card numbers.
- Requests: a popular HTTP library for making HTTP requests in Python. In the project, it sent requests to external web addresses and if the response of the status code was 200(oK), it extracted data. 
- Tabula: a Python library for extracting tables from PDFs into pandas DataFrames. It simplified the process of extracting structured data from PDF documents in S3 bucket.

# Project Environments
Integrated Development Environment (IDE): Visual Studio Code is chosen as the Integrated Development Environment (IDE) for its versatility and comprehensive list of extensions. This project used extensions such as Excel File Viewer, SQLTools, and TODO tree for enhanced functionality and productivity.
Conda Environment: The project is set up within a Conda environment, specifically using Conda version 23.11.0. Conda provides a robust environment management system, offering greater control over dependencies.

# Key Steps in Setting Connections
- Read credentials from secure storage and return them as dictionaries.
- Read the credentials dictionary and initialize a SQLAlchemy database engine to connect to AWS RDS database in the cloud.
- Use an SQLAlchemy database engine to obtain a list of tables in the AWS RDS database. Knowing the available tables helps in selectively choosing specific tables for extraction, avoiding the need to extract all tables.
- Upload the cleaned tables to a local PostgreSQL database for further analysis.
- Read a dictionary of API configuration details, including API key and two endpoints, and make secure and authenticated requests to external APIs.

# Key Steps in Data Extraction
- Select tables by their names from the available list.
- Extract users and orders information and convert them into Pandas DataFrames.
- Extract payment cards information from a PDF document stored in an AWS S3 bucket, using tabula-py Python package converting it into a Pandas DataFrame.
- Obtain a number of stores.
- Retrieves data from each of the available stores from the API and converts them into a Pandas DataFrame.
- Extract company stores through the use of an API, using two GET methods. One returns the number of stores in the business, and the other retrieves an individual store's data given a store number.
- Use the boto3 package to download and extract the products' information returning a Pandas DataFrame while being logged into the AWS CLI.
- Extract datetimes information for all individual company sales in a JSON file from an AWS S3 bucket.

# Key Steps in Data Cleaning and Transformation with Pandas
- Replace 'NULL' string values with NULL.
- Check which rows contain only N/A or values in an unintelligible format and removing them.
- Convert day, months, years, and timestamps from different formats into a consistent format using pandas new method 'format='mixed'.
- Remove unnecessary letters from country codes and continent names in stores data.
- Remove non-number characters from columns with numeric values such as card number or staff numbers.
- Drop columns with junk data.
- Convert products' weights provided in kg, k, oz, ml, g to a decimal value in kilograms.

# Key Steps in Dataset Uploading
- Initiate SQLAlchemy engine to connect to a local PostgreSQL database.
- Check if the output of the cleaning process produces a valid dataframe (not empty).
- Upload DataFrames to the PostgreSQL database under specific name.

# Key Steps in Security
- Store Credentials Securely: External and internal credentials to establish connections to AWS RDS, S3 buckets and POstgreSQL database are stored in separate YAML files, keeping them outside of the scripts. Not hardcoding credentials into the script adds an extra layer of security.
- Use SQLAlchemy Engines: Using SQLAlchemy engines for database connections provides a high-level API and handles many security aspects.
- Gitignore: Listing credentials in .gitignore to prevent them from being inadvertently shared when pushing code to version control.
- Requests Package: The use of this Python package for HTTP requests handles many security considerations by default, such as handling redirects and verifying SSL/TLS.
- File Handling with with Statement:  Opening and closing credential files with a with statement ensures that resources are properly released, and the files are closed after use.
- HTTPS Usage: use of HTTPS instead of HTTP for secure communication with external APIs and web services.

# Key Steps in Creating Database Schema
- Convert datatypes.
- Set VARCHAR length to an integer representing the maximum length of the values in specific columns.
- Create, rename, and merge columns.
- Change 'NULL' string values in a specific row and a specific column to N/A to avoid errors.
- Create a new human-readable column for classifying products' weight so they can quickly make decisions on delivery weights.
- Update the columns in the dimensional tables with a primary key that matches the same column in the orders_table, setting foreign keys.

# Key Steps in Business Analysis in PGAdmin
Perform complex SQL data queries to extract business insights using PL/pgSQL (Procedural Language for PostgreSQL). This allows for more complex logic and control structures than standard SQL.

- Dynamic SQL generation for altering column types based on calculated values.
- Window functions and LAG for advanced analytical tasks.
- Common Table Expression (CTE) with (WITH clause) for defining temporary result sets to simplify complex queries.
- UUID handling for Universally Unique Identifiers using PostgreSQL's built-in UUID type.
- Timestamp manipulations using functions like TO_TIMESTAMP.
- Datatype alteration and Local variables/declarations:
- Data standardisation by applying consistent formatting, units, or representations.
- Joining tables to combine data from multiple tables for comprehensive analysis.
- Aggregate functions(SUM, COUNT, AVG, etc.) to perform statistical calculations on data.
- Group data using the GROUP BY clause for insights based on specific categories.
- Apply ordering (ORDER BY) to present results in a meaningful way.
- Rounding functions (ROUND) for numerical precision.
- Use casting (CAST) to convert data types.
- Conditional Logic with CASE and filtering with WHERE to narrow down results based on specific conditions.
- VARCHAR column Length adjustment on the calculated maximum length to prevent data truncation.

# Sample Business Queries: 
1. The Operations team would like to know all countries the company operates in and which country has the most stores. 
2. The business stakeholders are considering closing some stores before opening more in other locations. They would like to know which locations currently have the most stores.
3. The company wants to know which months have produced the largest amount of sales.
4. The company is looking to increase its online sales. They want to know how many products were sold and the amount of sales made for online and offline purchases.
5. The sales team wants to identify which store types generate the most revenue. They would like to know the total and percentage of sales coming from each of the different store types.
6. The company stakeholders want assurances that the company has been doing well recently. They want to know which months across all years where it has most sales historically.
7. The operations team would like to know the overall staff numbers in each location around the world.
8. The sales team is looking to expand their territory in Germany. They need information about the type of store that is generating the most sales in Germany.
9. Sales would like to get an accurate metric for how quickly the company is making sales. They want to know the average time taken between each sale grouped by year.

# File Structure of the Project
1. The `data_extraction.py` - this script creates a class named DatabaseExtractor that works as a utility class. It contains methods to extract data from RDS tables, PDF, and JSON, and CSV files in S3 buckets. 
2. The `database_utils.py` - this script creates a class named DatabaseConnector that is used to connect with and upload data to the database.
3. The `data_cleaning.py` - its `DataCleaning` class is designed to encapsulate method for cleaning DataFrames from separate sources. 
4. The `main.py` script is structured around classes and methods, aligning with OOP principles. It orchestrates the overall data processing workflow by calling functions from other scripts.
5. YAML files - these files contain connection credentials.
6. `.gitignore` - this folder contains YAML files with credentials to prevent the sharing of sensitive information.
7. Images of SQL queries and their output.

# Conclusion:
I enjoyed this project, learning new concepts from programming to database connection setp up, extracting data. I found the task of cleaning especially satisfying making sure not a single row of dataframe goes missing due to a typo in just one column. There were certainly challenges adn the help from AICORE support engineers in the evenings and weekends was valuable, especially since advice provided on stackflow exchnage or ninja sometimes contained functions that have been deprecated.