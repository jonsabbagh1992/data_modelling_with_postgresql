import os
import glob
import pandas as pd
from create_connection import create_database_connection
import sql_queries as sql

def process_song_file(cur, filepath):
    '''
    Description: This file reads the song JSON files in the filepath (data/song_data)
    to get song and artist data, which are used to load into the song and artist dim
    tables.
    
    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_cols].drop_duplicates().values.tolist()[0]
    cur.execute(sql.song_table_insert, song_data)
    
    # insert artist record
    artist_cols = ['artist_id', 'artist_name', 'artist_location',
                   'artist_latitude', 'artist_longitude']
    artist_data = df[artist_cols].drop_duplicates().values.tolist()[0]
    cur.execute(sql.artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts'] 
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({column_name: time_data for column_name, time_data in zip(column_labels, time_data)})

    for i, row in time_df.iterrows():
        cur.execute(sql.time_table_insert, list(row))

    # load user table
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_cols].drop_duplicates()
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sql.user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(sql.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = (row['ts'], row['userId'], row['level'], songid,
                                         artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(sql.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Description: Wrapper function to run both processing functions for
    song_data (data/song_data) and log_data (data/log_data).
    
    JSON files are collected from the directories and passed through the two
    functions.
    
    Arguments:
        cur: the cursor object.
        conn: the connection object.
        filepath: log data file path. 
        func: processing function (process_song_files or process_log_files).
        
    Returns:
        None    
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for index, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{index}/{num_files} files processed.")


def main():
    conn = create_database_connection()
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()