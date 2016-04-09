--create table of unique users
--drop table if already exists
DROP TABLE IF EXISTS users;
CREATE TABLE users(userid INT);
INSERT OVERWRITE TABLE users
SELECT DISTINCT userid FROM ratings
LIMIT 10;

--create table of top rated movies from average rating that have at least
--30 ratings
DROP TABLE IF EXISTS top_m;
CREATE TABLE top_m(movieid INT, avg_rating DOUBLE);
INSERT OVERWRITE TABLE top_m
SELECT movieid, avg_rating
FROM(
	--get average and no. of ratings per movie
	SELECT movieid, avg(rating) AS avg_rating, count(rating) AS count_rating
	FROM ratings
	GROUP BY movieid
) x
WHERE count_rating >= 30
ORDER BY avg_rating DESC
LIMIT 100;

--list top 5 rated movies not seen by user
DROP TABLE IF EXISTS unseen;
CREATE TABLE unseen(userid INT, movieid INT);
INSERT OVERWRITE TABLE unseen
SELECT userid, movieid
FROM(
	--get movie's ranking in the user's list
	SELECT userid, movieid, row_number() OVER (PARTITION BY userid ORDER BY avg_rating DESC) AS rank
	FROM(
		--get table of movies not seen by user
		SELECT top_m_users.*
		FROM(
			--create table of all possible user/movie combos from top_m
			SELECT *
			FROM users
			CROSS JOIN top_m
		) top_m_users
		LEFT OUTER JOIN ratings r
		ON top_m_users.movieid=r.movieid AND top_m_users.userid=r.userid
		WHERE r.movieid IS NULL --exclude cases where user HAS seen the movie
	) x
) y
WHERE rank <= 5;

--output movies
--we're going to store it in the hive directory for now, you can change this
--look for it when you have run the script to see the results
INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/movierecs/'
SELECT u.userid,m.name
FROM unseen u
JOIN movies m
ON u.movieid=m.movieid;
