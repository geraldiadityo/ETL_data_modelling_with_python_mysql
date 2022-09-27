import os
import glob
import pandas as pd
import pymysql as sql
from sql_queries import *


def process_song_file(cur,filepath):
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])
    for value in df.values:
        nums_song, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title,duration, year = value

        #insert data artist
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert,artist_data)

        #insert data song
        song_data = (song_id,title,artist_id, year, duration)
        cur.execute(song_table_insert, song_data)
    
    print(f"Record inserted for file {filepath}")


def process_data(cur, conn, filepath, func):
    all_files = []
    for root, dirs ,files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))
    for i, datafile in enumerate(all_files,1):
        func(cur,datafile)
        conn.commit()
        print("{}/{} files process.".format(i,num_files))

def main():
    conn = sql.connect(host="127.0.0.1",user="admin",passwd="Ge@140019",db="sparkifydb",port=3306)
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    conn.close()


if __name__ == "__main__":
    main()
    print("\n\nFinished processing !!\n")
