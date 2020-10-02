# Sparkify Project: Create Data Warehouse in Amazon Redshift

- Data modeling with Postgres and building an ETL pipeline using Python. 


## Introduction

Sparkify is a music company that has gone through a lot of growth and now wants to move their processes and data onto the cloud. They have already uploaded all their data to an S3 bucket which is split into user activity as well as metadata on songs. 

I created an ETL pipeline that takes this raw data from S3, stages them in Amazon Redshift, and then create the fact and dimension tables. 


## Schema
The schema is 5 tables. The information for songs and artists table came from the 
song_data file. The log_data folder contains information for the time table and the users table. For the time table, the information is extracted from one column containing the timestamp for user plays. The songplay table 
The time table comes from log The information for the songplay table comes from the other 4 tables. A SQL query was written to get song_id and artist_id for songs that were in the song_data file, the songs with no matches simply have NONE value. 

### Entity Relationship Diagram

![Schema Diagram](https://user-images.githubusercontent.com/53429726/94948984-35e5b800-04ae-11eb-8c67-e8c5b36e2b6b.png)




## ETL pipeline

Processing the data for the `songs` and `artists` dimension tables came directly from the song_data folder. Each JSON file contained one row containing information for one song with the corresponding artist information. 


### Create Tables For Database
In create_tables.py, we connect to our database using a config file that contains our login information. To start, we drop tables if they already exist in the database. This prevents errors resulting from duplicate tables. We create tables based on our code from sql_queries.py. 

### ETL Pipeline: Copy Data from S3 and Format to Tables
In etl.py, we log into the S3 database using our config file. We then create a staging tables for our raw data. Raw data is stored in a JSON file and needs to be converted to the right formats. Once the data is copied into the staging tables, we create tables and insert the formatted data into our schema. Finally, we close connection if everything is ran correctly.



## How to run python scripts
1. Fill in login information in provided dwh.cfg file
2. Enter and run in jupyter notebook: !python create_tables.py
3. Enter and run in jupyter notebook: !python etl.py


## Conclusion
The project was a great way to navigate the Amazon Redshift and S3. The creation of staging tables allows raw data to be converted to the right formats for insertion into database tables. Sparkify can now grow data, save onto S3, and update their tables as quick as they prefer through AWS infrastructure. Analysts can now pull pull data directly from Redshift.