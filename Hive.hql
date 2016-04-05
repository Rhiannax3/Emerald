
CREATE EXTERNAL TABLE movie
(movieID string, movieName string, movieGenres string)
LOCATION 'user/hive/ml-latest/movies/'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE tag
(userID string, movieID string, tagName string, tagTimestamp timestamp, userGender string)
LOCATION 'user/hive/ml-latest/tags/'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE rating
(userID string, movieID string, ratingStar int, ratingTimestamp timestamp)
LOCATION 'user/hive/ml-latest/ratings/'
tblproperties("skip.header.line.count"="1");




