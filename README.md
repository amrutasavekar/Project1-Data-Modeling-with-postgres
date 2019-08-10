# Project1-Data-Modeling-with-postgres

# Introduction:

Sparkify is the startup and they want to analyse the music data collected using music streaming app. Purpose of this project is to understand which songs currently users are listening by collecting data from JSON log files and by creating the database schema and ETL pipeline for the analysis.

# Project Dataset:

There are two datasets provided for this project one is song dataset and another one is log data , files in both dataset are in JSON format.
Song dataset has metadata about song and artist of the song and log data has simulated activity logs from a music streaming app based on specified configurations.

# Star Schema:

Star Schema is provided in StarSchema.png file which has ,
1.Fact table: Songplay
2.Dimension tables: songs,artists,time,users.


# Instructions:

Note: sql_queries is the python file which is imported by all the below scripts and has all the sql queries used throughout the project.

1. Run create_tables.py script which will drop previous tables and
   create new ones.
2. Run etl.py script which has complete ETL process which will extract   
   data from JSON , transform it into dataframe and then load that into postgresql    
   tables.
3. Analysis file provides the quick analysis of song and log data files.


# ETL Pipeline:
Song data files  are in JSON format and has metadata about song and artists. 
In etl.py script , every file from song data directory is read and is transformed from
JSON to dataframe , after this data from dataframe is then loaded into postgres SQL table using insert queries from sql_queries.py.
song_id and artist_id are the primary keys of the respective tables as both has to be unique and not null for every song.

Log data files are also in JSON format and has data collected by event simulator.
etl.py script reads every file from log data and then tranform it into python dataframe.
Log data has data which gets loaded into three tables time,users and songplay.

Each and every timestamp from the dataframe is converted into datetime format , from that week,hour,weekday,year are extracted and then loaded into time table. 
Timestamp is the primary key here to avoid duplication.

user_id,name,gender and level of the user are extracted from log data and then loaded into users table. Here user_id is the primary key, in case there is user_id which is duplicated then level field for that user is updated with new value.

For songplay data first artist_id and song_id are extracted from artists and songs table
according to song and then by extracting start_time, user_id,session_id, location, user_agent from log data the record is inserted into songplay table. songplay_id is the primary key for this which will be autoincremented with new record.

# Results:
Run script test.ipnyb to check the tables.
Execute Data_Analysis.ipnyb script. 

After executing script Data_Analysis.ipynb it has observed that there is just one song that is common between log data and song data.
Hence given datasets are insufficient to perform specific analysis on the dataset. 
Song dataset needs to have more metadata about songs.

