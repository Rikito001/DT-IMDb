-- Graf 1 - Top 5 countries with most movies
SELECT 
    m.country, 
    COUNT(*) AS movie_count
FROM dim_movies m
GROUP BY m.country
ORDER BY movie_count DESC
LIMIT 5;

-- Graf 2 - Top 5 longest movies
SELECT title, duration 
FROM dim_movies 
WHERE duration IS NOT NULL 
ORDER BY duration DESC 
LIMIT 5;

-- Graf 3 - Top 10 movies with most USD revenue
SELECT title, 
       CAST(REPLACE(REPLACE(worldwide_gross_income, '$ ', ''), ' ', '') AS INTEGER) as revenue
FROM dim_movies 
WHERE worldwide_gross_income IS NOT NULL 
AND worldwide_gross_income NOT LIKE '%NULL%'
AND worldwide_gross_income LIKE '$%'
ORDER BY CAST(REPLACE(REPLACE(worldwide_gross_income, '$ ', ''), ' ', '') AS INTEGER) DESC 
LIMIT 10;

-- Graf 4 - Top 10 tallest directors
SELECT DISTINCT name, height 
FROM dim_directors 
WHERE height IS NOT NULL 
ORDER BY height DESC 
LIMIT 10;

-- Graf 5 - Top 5 directors with the most movies
SELECT d.name, COUNT(DISTINCT fr.dim_movies_movie_id) as movie_count
FROM dim_directors d
JOIN fact_ratings fr ON d.director_id = fr.dim_directors_director_id
GROUP BY d.name
ORDER BY movie_count DESC
LIMIT 5;
