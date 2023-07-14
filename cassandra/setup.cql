CREATE KEYSPACE IF NOT EXISTS spotify_streaming
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spotify_streaming;

CREATE TABLE IF NOT EXISTS streams(
    user_name text,
    user_address text,
    user_age int,
    user_nationality text,
    song_artist text,
    song_album_name text,
    song_name text,
    song_genre text,
    song_length float,
    song_played_at timestamp,
    PRIMARY KEY(user_name, song_played_at)
) WITH CLUSTERING ORDER BY (song_played_at DESC);
