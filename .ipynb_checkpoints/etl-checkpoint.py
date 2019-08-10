import os
import io
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

#Declaring global dataframes to copy into SQL tables.

s_columns=['song_id', 'title', 'artist_id', 'year', 'duration']
song_df=pd.DataFrame(columns=s_columns)

a_columns=['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
artist_df=pd.DataFrame(columns=a_columns)

t_columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
time_df=pd.DataFrame(columns=t_columns)

u_columns=['userId', 'firstName', 'lastName', 'gender', 'level']
user_df=pd.DataFrame(columns=u_columns)

splay_columns=['songplay_id','ts','userId','level','songid','artistid','sessionId','location','userAgent']
songplay_df=pd.DataFrame(columns=splay_columns)

def format_list(df):
    # Format the float number to have the precision of 5
    unfmt_data=df.values.tolist()
    data=[]
    for item in unfmt_data:
        if isinstance(item,float):
            data.append(round(item,5))
        else:
            data.append(item)
    return data
    
def process_song_file(cur, filepath):
    # Process songs files to store data into songs and artists table
    
    print(filepath)
    df = pd.read_json(filepath, lines=True)
    df=df.iloc[0]
 
    
    single_song_df=df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data=format_list(single_song_df)    
    
    #Appending the data to master dataframe
    song_df.loc[len(song_df), :] = song_data
    
    single_artist_df=df[['artist_id','artist_name','artist_location',      'artist_latitude','artist_longitude']]
    
    #Appending the data to master dataframe
    artist_data=format_list(single_artist_df)
    artist_df.loc[len(artist_df), :] = artist_data

    
def process_log_file(cur, filepath):
    ''' Process log file to populate time,users and sonplay tables '''
    # open log file
    df =  pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df =df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    #Create time dataframe
    t = df.copy()
    
    #Create the row in the time data from timestamp
    time_data=(t.ts,t.ts.dt.hour,t.ts.dt.day,t.ts.dt.dayofweek,t.ts.dt.month,t.ts.dt.year,t.ts.dt.weekday)
    #Create the dictionary to map field names and  time data.
    time_dict={"start_time":t.ts,
               "hour":t.ts.dt.hour,
               "day":t.ts.dt.day, 
               "week":t.ts.dt.dayofweek, 
               "month":t.ts.dt.month,
               "year":t.ts.dt.year,
               "weekday":t.ts.dt.weekday}
    
    single_time_df= pd.DataFrame.from_dict(time_dict)
    
    #Append the dataframe to master songplay dataframe.
    for index, row in single_time_df.iterrows(): 
        time_data=row.tolist()
        time_df.loc[len(user_df), :] = time_data
        

    #Create users dataframe   
    single_user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    #Appending user information to master dataframe.
    for index, row in single_user_df.iterrows(): 
        user_data=row.tolist()
        user_df.loc[len(user_df), :] = user_data
        
 
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # Create songplay record
        songplay_data =[index,str(row.ts),str(row.userId),str(row.level),str(songid),str(artistid),str(row.sessionId),str(row.location),str(row.userAgent)]
        songplay_df.loc[len(songplay_df), :] = songplay_data
        songplay_df.reset_index()
        

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def bulk_copy(cur,conn,copy_command,df):
    #Bulk insert all the data from dataframes into database table.
    df.fillna(0,inplace=True)
    #df=df.drop_duplicates(keep='last')
    print(df)
    print("Duplicates removed")
    #Copying data to buffer and convert it into csv file.
    buf=io.StringIO()
    df.to_csv(buf, sep='\t',index=False)
    buf.seek(0)
    cur.copy_expert(copy_command,buf)
    conn.commit()
        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
   
    #bulk_copy song data into songs table
    bulk_copy(cur,conn,song_copy,song_df)
    print("Song table has been successfully loaded!")
        
    #bulk copy artist data into artists table
    bulk_copy(cur,conn,artist_copy,artist_df)
    print("Artist table has been successfully loaded!")
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    #bulk copy time data into time table
    bulk_copy(cur,conn,time_copy,time_df)
    print("Time table has been successfully loaded!")
    
    #bulk copy user data into users table
    print(user_df)
    bulk_copy(cur,conn,user_copy,user_df)
    print("User table has been successfully loaded!")
    
    #bulk copy songplay data into songplay table
    bulk_copy(cur,conn,songplay_copy,songplay_df)
    print("Songplay table has been successfully loaded!")
    
    conn.close()



if __name__ == "__main__":
    main()

