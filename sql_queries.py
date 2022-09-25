# hapus table

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# membuat table

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL NOT NULL PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time (start_time),
        user_id INT REFERENCES users (user_id),
        level VARCHAR(100) NOT NULL,
        song_id VARCHAR(100) REFERECES songs (song_id),
        artist_id VARCHAR(100) REFERENCES artists (artists_id),
        session_id INT NOT NULL,
        location VARCHAR(100),
        user_agent TEXT,
    );
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users(
        user_id INT NOT NULL PRIMARY_KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        gender CHAR(1),
        level VARCHAR(100) NOT NULL,
    );
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(100) NOT NULL UNIQUE PRIMARY KEY,
        title VARCHAR(100),
        artist_id VARCHAR(100) REFERENCES artists (artist_id),
        year INT CHECK (year >= 0),
        duration FLOAT,
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(100) NOT NULL UNIQUE PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6),
    );
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INT NOT NULL CHECK (hour >= 0),
        day INT NOT NULL CHECK (day >= 0),
        week INT NOT NULL CHECK (week >= 0),
        month INT NOT NULL CHECK (month >= 0),
        year INT NOT NULL CHECK (year >= 0),
        weekday VARCHAR(100) NOT NULL,
    );
    """
)

#INSERT RECORD

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
    """
)

songplay_table_insert = (
    """
    INSERT INTO songplays VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
    """
)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
    """
)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longtitude) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE SET
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longtitude = EXCLUDED.longtitude;
    """
)

time_table_insert = (
    """
    INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
    """
)

#CARI LAGU

song_select = (
    """
    SELECT song_id, artist.artist_id
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s;
    """
)

# QUERY LIST
create_table_queries = [user_table_create,artist_table_create,song_table_create,time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop,user_table_drop,artist_table_drop,time_table_drop,songplay_table_drop]