--print out the year and the average rating per year
SELECT year, AVG(r) AS ar FROM(	
	--regular expression extracts the year section of each movie (YYYY) 
	SELECT regexp_extract(name, '\\((\\d\\d\\d\\d)\\)',1) AS year, rating AS r from movies
	JOIN ratings
	ON movies.movieid=ratings.movieid
	) x
GROUP BY year
--show highest rated years at the top
ORDER BY ar DESC;
	

