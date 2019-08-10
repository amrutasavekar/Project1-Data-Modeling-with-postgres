# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplay
(
songplay_id serial PRIMARY KEY,
start_time TIMESTAMP ,
user_id INT NOT NULL,
level VARCHAR(50),
song_id VARCHAR(150),
artist_id VARCHAR(150),
session_id VARCHAR(150),
location VARCHAR(150),
user_agent VARCHAR(150));
""")

user_table_create = ("""
   CREATE TABLE users(
   user_id INT PRIMARY KEY NOT NULL,
   first_name VARCHAR (150) NOT NULL,
   last_name VARCHAR (150) NOT NULL,
   gender VARCHAR(100),
   level VARCHAR (155) NOT NULL);
   """) 


song_table_create =("""CREATE TABLE songs(
   song_id VARCHAR(255) PRIMARY KEY NOT NULL,
   title VARCHAR (255) NOT NULL,
   artist_id VARCHAR (255) ,
   year INT NOT NULL,
   duration NUMERIC(17,5));   
   """)

artist_table_create = ("""CREATE TABLE artists(
   artist_id VARCHAR(255) PRIMARY KEY NOT NULL,
   name VARCHAR (255) NOT NULL,
   location VARCHAR (255),
   latitude NUMERIC(17, 5) , 
   longitude NUMERIC(17, 5));
    """)

time_table_create = ("""CREATE TABLE time(
start_time TIMESTAMP PRIMARY KEY NOT NULL ,
 hour INT ,
 day INT,
 week INT,
 month INT,
 year INT,
 weekday INT
);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay(start_time,user_id,level,song_id,artist_id,
session_id,location,user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")
user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name ,gender,level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT(user_id)
DO
UPDATE
SET level =%s ;
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id ,year,duration) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING """)

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
""")

# FIND SONGS
song_select = ("""SELECT songs.song_id,artists.artist_id
FROM songs  JOIN artists ON
songs.artist_id = artists.artist_id WHERE songs.title=%s AND artists.name=%s AND
songs.duration=%s;""")


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop,song_table_drop, artist_table_drop, time_table_drop]
