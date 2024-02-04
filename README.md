<p align="left">
    <h1 align="left">Multinational Retail Data Centralisation</h1>

<p align="left">
   <img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=plastic&logoColor=white" alt="license">
	<img src="https://img.shields.io/github/last-commit/ZCHAnalytics/multinational-retail-data-centralisation188?style=plastic&color=0080ff" alt="last-commit">
	<img src="https://img.shields.io/github/languages/top/ZCHAnalytics/multinational-retail-data-centralisation188?style=plastic&color=0080ff" alt="repo-top-language">
	<img src="https://img.shields.io/github/languages/count/ZCHAnalytics/multinational-retail-data-centralisation188?style=plastic&color=0080ff" alt="repo-language-count">
   <img src="https://img.shields.io/github/repo-size/ZCHAnalytics/multinational-retail-data-centralisation188?style=plastic">

<p>
<p align="left">
		<em>Developed with the tools below.</em>
</p>
<p align="left">
	<img src="https://img.shields.io/badge/YAML-CB171E.svg?style=plastic&logo=YAML&logoColor=white" alt="YAML">
	<img src="https://img.shields.io/badge/PostgreSQL-4169E1.svg?style=plastic&logo=PostgreSQL&logoColor=white" alt="PostgreSQL">
	<img src="https://img.shields.io/badge/Python-3776AB.svg?style=plastic&logo=Python&logoColor=white" alt="Python">
	<br>
	<img src="https://img.shields.io/badge/pandas-%23150458.svg?style=plastic&logo=pandas&logoColor=white" alt="Pandas">
	<img src="https://img.shields.io/badge/Amazon_AWS-FF9900?style=plastic&logo=amazonaws&logoColor=white alt="Amazon AWS">
	<img src="https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=plastic&logo=jupyter&logoColor=white alt="Jupyter">
</p>
<hr>


# 🔗 Content
> - [📍 Project Snapshot: Two Parts](#overview)
>-  [🔌 Steps in Establishing Secure Connections to Data Sources](#connect)
>-  [📥🧹📤 A Closer Look at ETL (Extract, Transform, Load)](#etl)
> - - [E. Steps in Data Extraction](#extract) 
> - - [T. Steps in Cleaning and Transformation with Pandas](#transform)
> - - [L. Steps in Loading to PostgreSQL](#load)
> - [👁️‍🗨️ Turning Chaos into Business Insights with PostgreSQL](#postgresql)
> - - [⭐ Steps in Creating Database Schema](#schema)
> - - [❔ SQL Data Queries for Business Analysis](#sql)
> - [⚙️ Project Programming Language, Environment and Packages](#env)
> - [📂 Project File Structure](#files)
> - [🔐 Security Considerations](#security)
> - [📄 License](#license)
> - [👏 Acknowledgments](#kudos)

---
<a name="overview"></a> 
# Project Snapshot: Two Parts
Welcome to the hub of retail data centralisation! 

In this project, I embark on building a complete data solution for a multinational organisation, covering every step from setting up a secure project environment and connections, as well as data acquisition and analysis. 

The first part of the project is run from Visual Studio Code and involves developing scripts in Python programming language to securely extract large datasets from multiple external sources and utilising the power of Pandas for data cleaning.

The second part is based on PostgreSQL where data chaos is turned into structured business insights 🧑‍💼.

<a name="connect"></a>
# Steps in Establishing Secure Connections
- Reading credentials from secure storage outside the code with PyYaml and return them as dictionaries.
- Deploying a SQLAlchemy database engine to connect to AWS RDS database in the cloud, obtaining a list of tables to aid in selective table extrraction.
- Loading the cleaned tables to a local PostgreSQL database for further analysis.
- Making secure and authenticated requests to external APIs using a dictionary of API configuration details (API key and endpoints).

<a name="etl"></a>
# A Closer Look at Extraction, Tranform and Load (ETL) process:

<a name="extract"></a>
## 🤏 Steps in Data Extraction
- Selecting tables by their names from the available list.
- Extracting users and orders information as Pandas DataFrames.
- Extracting payment cards information from a PDF document stored in an AWS S3 bucket, using tabula package.
- Obtaining company stores data through API requests.
- Downloading public information using boto3 package.
- Extracting datetimes information for all individual company sales in a JSON file from an AWS S3 bucket.

<a name="transform"></a>
## 🧹 Steps in Data Cleaning and Transformation with Pandas 
- Replacing 'NULL' string values with N/A.
- Removing rows that only contain N/A or unintelligible values.
- Standardising date and time formats.
- Cleaning country codes and continents names.
- Removing non-number chacarcters from numeric columns, where appropriate (e. card number 💳).
- Dropping columns with junk data.
- Converting products' weights ⚖️ provided in kg, k, oz, ml, g to a consistent decimal value in kilograms.

<a name="load"></a>
## 🏋️  Steps in Datasets Uploading
- Initiating an SQLAlchemy engine for local PostgreSQL database connection.
- Validating if the output of the cleaning process produces a valid dataframe (not empty).
- Uploading DataFrames to the PostgreSQL database under specific names.

<a name="postgresql"></a>
# Turning Chaos into Business Insights with PostgreSQL

<a name="schema"></a>
## ⭐  Steps in Creating Database Schema 
- Converting datatypes and setting VARCHAR length to an integer representing the maximum length of the values in specific columns.
- Creating, renaming, and merging columns.
- Handling 'NULL' values for a specific store categories and adding a new human-readable column for product weight classification for Delivery Team.
- Updating tables' primary and foreign keys.

<a name="sql"></a>
## SQL Data Queries for Business Analysis
Performing complex SQL data queries to extract business insights using PL/pgSQL (Procedural Language for PostgreSQL). 

- Dynamic SQL generation for altering column types.
- Using Window functions, LAG and CTE for advanced analytical tasks.
- Handling UUID, timestamp manipulations and datatype alterations.
- Data standardisation, joining tables, aggregate functions(SUM, COUNT, AVG, etc.) for statistical calculations.
- Grouping data, applying ordering, rounding, casting and conditional logic for precise analysis.

<a name="queries"></a>
### Business Queries Performed
1. 🗺️ The Operations team would like to know all countries the company operates in and which country has the most stores. 
2. 🏤 The business stakeholders are considering closing some stores before opening more in other locations. They would like to know which locations currently have the most stores.
3. 📆  The company wants to know which months have produced the largest amount of sales.
4. 💻 The company is looking to increase its online sales. They want to know how many products were sold and the amount of sales made for online and offline purchases.
5. 🛒 The sales team wants to identify which store types generate the most revenue. They would like to know the total and percentage of sales coming from each of the different store types.
6. 📊 The company stakeholders want assurances that the company has been doing well recently. They want to know which months across all years where it has most sales historically.
7.  🚶 The operations team would like to know the overall staff numbers in each location around the world.
9. ![image](https://github.com/ZCHAnalytics/multinational-retail-data-centralisation188/assets/146954022/d59976b1-e31a-47de-9429-16d1e0e7d92d) The sales team is looking to expand their territory in Germany. They need information about the type of store that is generating the most sales in Germany.
10. 🕒 Sales would like to get an accurate metric for how quickly the company is making sales. They want to know the average time taken between each sale grouped by year.

<a name="env"></a>   
# Project Programming Language, Environment and Packages
The project starts on Python 3.12.0, employing object-oriented programming (OOP) principles. Key Python packages include:
- PyYAML: facilitating secure storage of credentials outside of hard code.
- SQLAlchemy: a SQL toolkit and ORM library for secure databse connections. 
- Pandas: data manipulation and analysis library used for efficient data manipulation.
- Boto3: the AWS SDK that helps extract project data from AWS S3 buckets.
- Regular Expressions (re): Applied for pattern matchingm and was particularly useful for manipulate card number values.
- Requests: a popular HTTP library for making secure HTTP requests. 
- Tabula: a Python library for extracting tables from PDFs into pandas DataFrames. 

## Project Environments
IDE: Visual Studio Code provided versatility with the additional functionality of extensions Excel File Viewer, SQLTools, and TODO tree.
Conda: Version 23.11.0 was employed for better dependencies control.

<a name="files"></a>

# File Structure of the Project

| **Folder/File**                       | **Description**                                                                                                                                                                                                                            |
|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Data Management and ETL Process**   |                                                                                                                                                                                                                                            |
| `data_extraction.py`          	   | This script creates a class named DatabaseExtractor, serving as a utility class. It contains methods to extract data from RDS tables, PDFs, JSON, and CSV files in S3 buckets.                                                              |
| `database_utils.py`             	  | This script establishes a class named DatabaseConnector, used for connecting to and uploading data to the database.                                                                                                                       |
| `data_cleaning.py`                	| The `DataCleaning` class within this script is designed to encapsulate methods for cleaning DataFrames from various sources.                                                                                                                |
| `main.py`                         	| Structured around classes and methods, aligning with OOP principles, this script orchestrates the overall data processing workflow by calling functions from other scripts.                                                                 |
| **Database Design and SQL Queries** 	   |                                                                                                                                                                                                                                            |
| `scripts_star_schema_design.sql`	 | Responsible for creating a relational database.                                                                                                                                                                                            |
| `scripts_business_queries.sql`    	| This set of scripts generates insights for company departments and stakeholders.                                                                                                                  |
| `database_creation_visuals.ipynb` 	| A Jupyter Notebook with step by step design process and visual outputs from PGAdmin.                                                                                                                                         |
| `business_queries_visuals.ipynb`  	| Another Jupyter Notebook containing business queries in SQL with visuals from PGAdmin.                                                                                                                                     |
| `.gitignore`                        	| This file lists credential files (in YAML format) to prevent the sharing of sensitive information.                                                                                                                                  |

<a name="security"></a>
# Security Considerations
- Store Credentials Securely: Credentials are stored in separate YAML files, outside of the scripts, preventing hardcoding.
- SQLAlchemy Engines: Using SQLAlchemy engines provides a high-level API, enhacing security.
- Gitignore: Prevents credentials from being inadvertently shared when pushing code to version control.
- Requests Package: Handles many security considerations by default.
- File Handling with (WITH) Statement: Ensures proper resources release and files closure.
- HTTPS Usage: Ensures secure communication with external APIs.

<a name="license"></a>
# License: MIT

<a name="kudos"></a>
# Acknowledgements
Special thanks to my AICore support engineers A., B., H., I., I., J., K., M., M., V., W., for their invaluable assistance and latest hacks! This journey wouldn't be as epic without their guidance. 🏆 🏅

This journey also owes its success generous knowldedge-sharing by contributors to these various platforms:
- stackoverflow
- atlassian
- reddit
- jetbrains
- codeacademy
- dba.stackexchange
- realpython
- nbshare.io
- capitalone
- saturncloud.io
- hevodata.com
- sentry.io
- restapp.io
- appsloveworld.com
- postindustria.com
- geeksforgeeks
- data-flair.training
... and many others
  
.... And lastly, the wegetanystock website for helping verify product prices for Nescafe Dolce Gusto and pet food where units of measurements were missing! 
   
