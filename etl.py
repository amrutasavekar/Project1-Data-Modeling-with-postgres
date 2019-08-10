import os
import io
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def format_list(df):
    """" Format the float number to have the precision of 5 """
    
    unfmt_data=df.values.tolist()
    data=[]
    for item in unfmt_data:
        if isinstance(item,float):
            data.append(round(item,5))
        else:
            data.append(item)
    return data
    
def process_song_file(cur, filepath):
    """" Process songs files to store data into songs and artists table"""
    
    print(filepath)
    df = pd.read_json(filepath, lines=True)
    df=df.iloc[0]
 
    song_df=df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data=format_list(song_df)  
    cur.execute(song_table_insert, song_data)
    
    
    artist_df=df[['artist_id','artist_name','artist_location',      'artist_latitude','artist_longitude']]
    artist_data=format_list(artist_df)
    cur.execute(artist_table_insert, artist_data)
   
    
def process_log_file(cur, filepath):
    """" Process log file to populate time,users and songplay tables by checking primary key constraints """
    
    df =  pd.read_json(filepath, lines=True)
    df =df.loc[df['page'] == 'NextSong']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.copy()
    time_data=[t.ts,t.ts.dt.hour,t.ts.dt.day,t.ts.dt.dayofweek,t.ts.dt.month,t.ts.dt.year,t.ts.dt.weekday]
    time_labels=["start_time","hour","day","week","month","year","weekday"]
    time_dict=dict(zip(time_labels, time_data))

    time_df= pd.DataFrame.from_dict(time_dict)
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))    

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    for i, row in user_df.iterrows():
        user_data=list(row)
        user_data.append(row.level)
        cur.execute(user_table_insert, user_data)
        
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        songplay_data =[row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        
        

def process_data(cur, conn, filepath, func):
    """" get all files matching extension from directory and process every file according name of the function passed as an arguement"""
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    print("Song table has been successfully loaded!")
    print("Artist table has been successfully loaded!")
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    print("Time table has been successfully loaded!")
    print("User table has been successfully loaded!")
    print("Songplay table has been successfully loaded!")
    
    conn.close()



if __name__ == "__main__":
    main()

