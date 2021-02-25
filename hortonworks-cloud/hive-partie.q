CREATE TABLE ratings (
userID INT,movieId INT,
rating INT,
time INT)
ROW FORMAT DELIMTED
FIELDS TERMINATED BY ‘\t’
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH ‘${env:HOME}/ml-100k/u.data’
OVERWRITE INTO TABLE ratings;
LOCATION ‘/data/ml-100k/u.data’

CREATE VIEW topMovieIds AS
select movieID, count(movieID) as ratinfCount from ratings group by movieID ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;

CREATE VIEW IF NOT EXISTS avgRATINGS as 
SELECT movieID, AVG(rating) as avgRating, COUNT(movieID) as ratingCount 
FROM ratings
GROUP BY movieID
ORDER BY avgRating DESC;

SELECT n.title, avgRating
FROM avgRatings t JOIN names n ON t.movieID = n.movieID
WHERE ratingCount > 10;
