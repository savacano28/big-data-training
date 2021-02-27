CREATE TABLE ratings (
userID INT,movieId INT,
rating INT,
time INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'u.data' OVERWRITE INTO TABLE ratings;
LOCATION ‘/data/ml-100k/u.data’

CREATE VIEW topMovieIds AS
select movieID, count(movieID) as ratingCount from ratings group by movieID ORDER BY ratingCount DESC;

SELECT n.title, ratingCount FROM topMovieIds t JOIN names n ON t.movieID = n.movieID;

CREATE VIEW IF NOT EXISTS avgRatings as 
SELECT movieID, AVG(rating) as avgRating, COUNT(movieID) as ratingCount 
FROM ratings
GROUP BY movieID
ORDER BY avgRating DESC;

SELECT n.title, avgRating
FROM avgRatings t JOIN names n ON t.movieID = n.movieID
WHERE ratingCount > 10;
