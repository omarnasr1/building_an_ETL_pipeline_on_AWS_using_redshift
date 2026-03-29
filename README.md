## building an ETL pipeline on AWS using redshift for `Sparkify`.
I have 2 types of `JSON files` resides in S3 the first one has data about the `song` and the `artist` and the second one has data about the `sessions` itself, I will use those 2 files to create a database whiche contains 5 tables in redshift
#### Project Repository files:
- sql_queries.py
contains all the `sql queries`, it will be used in the other files.
- create_tables.py 
drops and creates your tables.
- etl.py 
moving the data from JSON files to a `staging tables` comping those 2 tables to and transform the data to the main 5 tables of the database

#### How To Run the Project
First run CLUSTER.ipynb to get (endpoint & IAM role ARN).
Then to create the database you will run the `create_tables.py` frist then the `etl.py`

#### Database design

###### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong 
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

###### Dimension Tables

- users - users in the app
        user_id, first_name, last_name, gender, level
- ongs - songs in music database
        song_id, title, artist_id, year, duration
- artists - artists in music database
        artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
        
