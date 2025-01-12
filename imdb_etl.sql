-- DB and schema
CREATE DATABASE dragon_imdb_rework;
CREATE SCHEMA dragon_imdb_rework.staging;
USE SCHEMA dragon_imdb_rework.staging;

-- Staging
CREATE OR REPLACE TABLE movies_staging (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(250),
    worldwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);

CREATE OR REPLACE TABLE genres_staging (
    movie_id VARCHAR(10),
    genre VARCHAR(50),
    PRIMARY KEY (movie_id, genre)
);

CREATE OR REPLACE TABLE names_staging (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);

CREATE OR REPLACE TABLE ratings_staging (
    movie_id VARCHAR(10) PRIMARY KEY,
    avg_rating DECIMAL(3,1),
    total_votes INT,
    median_rating INT
);

CREATE OR REPLACE TABLE director_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    PRIMARY KEY (movie_id, name_id)
);

CREATE OR REPLACE TABLE role_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    category VARCHAR(10),
    PRIMARY KEY (movie_id, name_id, category)
);

-- Stage for CSV
CREATE OR REPLACE STAGE dragon_stage
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', '')
        EMPTY_FIELD_AS_NULL = TRUE
);

-- Data load
COPY INTO movies_staging
FROM @dragon_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

COPY INTO genres_staging
FROM @dragon_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

COPY INTO names_staging
FROM @dragon_stage/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL', ''))

COPY INTO ratings_staging
FROM @dragon_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

COPY INTO director_mapping_staging
FROM @dragon_stage/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

COPY INTO role_mapping_staging
FROM @dragon_stage/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

-- dim_movies
CREATE TABLE dim_movies AS
SELECT DISTINCT
    m.id AS movie_id,
    m.title,
    m.year,
    m.date_published,
    m.duration,
    m.country,
    m.worldwide_gross_income,
    m.languages,
    m.production_company
FROM movies_staging m;

-- dim_directors
CREATE TABLE dim_directors AS
SELECT DISTINCT
    n.id AS director_id,
    n.name,
    n.date_of_birth,
    n.height
FROM names_staging n
JOIN director_mapping_staging dm ON n.id = dm.name_id;

-- dim_genres
CREATE TABLE dim_genres AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY g.genre) AS genre_id,
    g.genre
FROM genres_staging g;

-- dim_roles
CREATE TABLE dim_roles AS
SELECT DISTINCT
    n.id AS role_id,
    rm.category AS role_name,
    n.name AS actor_name,
    n.date_of_birth,
    n.height
FROM names_staging n
JOIN role_mapping_staging rm ON n.id = rm.name_id;

-- dim_date
CREATE TABLE dim_date AS
SELECT DISTINCT
    CAST(date_published AS DATE) AS date_id,
    DAY(date_published) AS day,
    MONTH(date_published) AS month,
    YEAR(date_published) AS year
FROM movies_staging
WHERE date_published IS NOT NULL;

-- fact_ratings
CREATE TABLE fact_ratings AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY r.movie_id) AS rating_id,
    r.avg_rating AS rating,
    dg.genre_id AS dim_genres_genre_id,
    m.id AS dim_movies_movie_id,
    d.date_id AS dim_date_date_id,
    dr.director_id AS dim_directors_director_id,
    rl.role_id AS dim_roles_role_id
FROM ratings_staging r
LEFT JOIN movies_staging m ON r.movie_id = m.id
LEFT JOIN genres_staging g ON r.movie_id = g.movie_id
LEFT JOIN dim_genres dg ON g.genre = dg.genre
LEFT JOIN dim_date d ON m.date_published = d.date_id
LEFT JOIN director_mapping_staging dm ON r.movie_id = dm.movie_id
LEFT JOIN dim_directors dr ON dm.name_id = dr.director_id
LEFT JOIN role_mapping_staging rm ON r.movie_id = rm.movie_id
LEFT JOIN dim_roles rl ON rm.name_id = rl.role_id;

-- Staging deletion
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
