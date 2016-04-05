products = LOAD 'file' USING JsonLoader (pid:chararray, rating:float);

-- sample stuff

ratings = GROUP movies BY pid;

--avgMovieRatings = FOREACH movieRatings GENERATE AVG(movieRatings.rating);

avgRatings = FOREACH ratings {
	sum = SUM(products.rating);
	count = COUNT(products);
	GENERATE GROUP AS pid, sum / count AS avg; 
	-- GENERATE FLATTEN(products), sum/count as avg;w
};

tmp = foreach avgRatings {
	dif = (rating - avg) * (rating - avg);
	GENERATE *, dif as dif;
};

grp = GROUP tmp BY pid;
standard_tmp = FOREACH grp GENERATE FLATTEN(tmp), SUM(tmp.dif) as sqr_sum;
standard = FOREACH standard_tmp GENERATE *, sqr_sum / count AS variance, SQRT(sqr_sum / count as standard);