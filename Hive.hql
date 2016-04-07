
CREATE EXTERNAL TABLE RhiannaMovies
(movieID string, movieName string, movieGenres string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hive/ml-latest/movies/'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE RhiannaTags
(userID string, movieID string, tagName string, tagTimestamp timestamp, userGender string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hive/ml-latest/tags/'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE RhiannaRatings
(userID string, movieID string, rating int, ratingTimestamp timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hive/ml-latest/ratings/'
tblproperties("skip.header.line.count"="1");

-- Print out the average rating per year

SELECT movieYear, AVG (rating) AS RatingAverage FROM (

-- Use regular expression to extract the year of each movie (YYYY) 

	SELECT regexp_extract (movieName, '\\((\\d\\d\\d\\d)\\)', 1) AS movieYear, rating AS rating from RhiannaMovies
	JOIN RhiannaRatings
	ON RhiannaMovies.movieID=RhiannaRatings.movieID
	) x

GROUP BY movieYear

--show highest rated years at the top

ORDER BY ar DESC;


