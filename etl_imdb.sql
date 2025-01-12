CREATE DATABASE dragon_imdb;

CREATE SCHEMA dragon_imdb.staging;

USE dragon_imdb.staging;

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

-- Error handler
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

-- Transform
CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT
    m.id AS movie_id,
    m.title,
    m.year,
    m.date_published,
    m.duration,
    m.worldwide_gross_income,
    m.production_company
FROM movies_staging m;

CREATE OR REPLACE TABLE dim_dates AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY date_published) AS date_id,
    date_published,
    EXTRACT(YEAR FROM date_published) AS year,
    EXTRACT(MONTH FROM date_published) AS month,
    EXTRACT(DAY FROM date_published) AS day,
    DAYNAME(date_published) AS day_name,
    MONTHNAME(date_published) AS month_name
FROM movies_staging
WHERE date_published IS NOT NULL;

CREATE OR REPLACE TABLE dim_directors AS
SELECT DISTINCT
    n.id AS director_id,
    n.name AS director_name,
    n.date_of_birth,
    n.height
FROM names_staging n
JOIN director_mapping_staging dm ON n.id = dm.name_id;

CREATE OR REPLACE TABLE dim_genres AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY genre) AS genre_id,
    genre AS genre_name
FROM genres_staging;

CREATE OR REPLACE TABLE dim_roles AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY n.id, r.category) AS role_id,
    n.id AS name_id,
    n.name AS actor_name,
    r.category AS role_type
FROM names_staging n
JOIN role_mapping_staging r ON n.id = r.name_id;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.movie_id,
    r.avg_rating,
    r.total_votes,
    r.median_rating,
    m.id AS dim_movies_id,
    d.director_id AS dim_directors_id,
    g.genre_id AS dim_genres_id,
    dr.role_id AS dim_roles_id,
    dt.date_id AS dim_dates_id
FROM ratings_staging r
JOIN movies_staging m ON r.movie_id = m.id
LEFT JOIN director_mapping_staging dm ON m.id = dm.movie_id
LEFT JOIN dim_directors d ON dm.name_id = d.director_id
LEFT JOIN genres_staging gs ON m.id = gs.movie_id
LEFT JOIN dim_genres g ON gs.genre = g.genre_name
LEFT JOIN role_mapping_staging rm ON m.id = rm.movie_id
LEFT JOIN dim_roles dr ON (rm.name_id = dr.name_id AND rm.category = dr.role_type)
LEFT JOIN dim_dates dt ON m.date_published = dt.date_published;

-- Staging deletion
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
