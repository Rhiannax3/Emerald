
-- Register jar files

REGISTER '/home/rhianna/Downloads/elephant-bird/pig/target/elephant-bird-pig-4.14-SNAPSHOT.jar';
REGISTER '/home/rhianna/Downloads/elephant-bird/hadoop-compat/target/elephant-bird-hadoop-compat-4.14-SNAPSHOT.jar';
REGISTER '/home/rhianna/Downloads/json-simple-1.1.1.jar';​
REGISTER '/opt/hadoop-lzo-0.4.15.jar';

-- Load the data into Pig

ReviewRaw  = LOAD '/user/mike/complete.json' USING PigStorage (',') AS (reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:(), reviewText:chararray, overall:float, summary:chararray, unixReviewTime:int, reviewTime:datetime); 
ProductRaw = LOAD '/user/dan/metadata.json'  USING PigStorage (',') AS (productID:chararray, description:chararray, title:chararray, price:float, imageURL:chararray, related:(alsoBought:{tuple()}, buyAfter:{tuple()}), categories:{tuple(category)});

ReviewRaw  = LOAD '/user/mike/complete.json' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:(), reviewText:chararray, overall:float, summary:chararray, unixReviewTime:int, reviewTime:datetime); 
ProductRaw = LOAD '/user/dan/metadata.json'  USING com.twitter.elephantbird.pig.load.JsonLoader() AS (productID:chararray, description:chararray, title:chararray, price:float, imageURL:chararray, related:(alsoBought:{tuple()}, buyAfter:{tuple()}), categories:{tuple(category)});

-- Take a sample

SampleReview  = SAMPLE ReviewRaw 0.00001;
SampleProduct = SAMPLE ProductRaw 0.0001;

-- Dump to look at the data

dump SampleReview;
dump SampleProduct;

-- Create a new file with the sample data

STORE SampleReview  INTO 'SampleReviews4.json'  USING JsonStorage ();
STORE SampleProduct INTO 'SampleProducts4.json' USING JsonStorage ();

-- Load the sample data again

SampleReview  = LOAD '/user/rhianna/SampleReviews4.json'  USING PigStorage (',') AS (reviewerID:chararray, asin:chararray, reviewerName:chararray, helpful:(), reviewText:chararray, overall:float, summary:chararray, unixReviewTime:int, reviewTime:datetime); 
SampleReview  = LOAD '/user/rhianna/SampleReviews4.json'  USING PigStorage (',') AS (reviewerID:chararray, productID:chararray, reviewerName:chararray, helpfulness:(helpful:int, helpfulTotal:int), reviewText:chararray, overall:float, summary:chararray, timestampUNIX:int, reviewTime:datetime);
SampleProduct = LOAD '/user/rhianna/SampleProducts4.json' USING PigStorage (',') AS (productID:chararray, description:chararray, title:chararray, price:float, imageURL:chararray, related:(alsoBought:{tuple()}, buyAfter:{tuple()}), categories:{tuple(category)});

-- Check the structure of the data

DESCRIBE SampleReview;
DESCRIBE SampleProduct;

-- Remove all repeated entries



-- Group by productID then return top ten reviews

LimitReview = GROUP SampleReview BY reviewerID;
LimitReview = LIMIT LimitReview 10;

-- Filter to look at null values to decide what to do about them

SampleTest   = FILTER SampleReview BY (overall is null);
SampleFilter = FILTER SampleReview BY (overall is not null);

-- Filter to look at a specific individual's reviews

ProductSampleFilter = FILTER SampleProduct       BY asin is not null;
ProductSampleFilter = FILTER SampleProduct       BY price is not null;
OneProduct          = FILTER ProductSampleFilter BY asin == 'B00KNQ1CIU';
Cheap               = FILTER SampleProduct       BY (double)price < 2.0;

-- Create subsets focussing on products and group by productID to order the data set

ProductRatings = FOREACH SampleReview GENERATE reviewerID, asin, overall;
Products       = GROUP SampleProduct BY asin;

-- Calculate some statistics for product ratings (average, standard deviation, variance)

ProductRatings = FOREACH ProductRatings {
	sum   = SUM(ProductRatings.overall);
	count = COUNT(ProductRatings);
	GENERATE GROUP AS asin, sum / count AS ratingAverage;
};

Temp = FOREACH ratingAverage {
	difference = (overall - ratingAverage) * (overall - ratingAverage);
	GENERATE *, difference AS difference;
};

Temp       = GROUP Temp BY asin;
Temp       = FOREACH Temp GENERATE FLATTEN(Temp), SUM(Temp.difference) AS sumOfSquares;
Statistics = FOREACH Temp GENERATE *, sumOfSquares / count AS variance, SQRT(sumOfSquares / count) AS standardDeviation

-- Generate two subsets to merge

ReviewMerge = FOREACH SampleReview GENERATE reviewerID, asin, overall;
ProductMerge = FOREACH ProductRaw GENERATE asin, price;

-- Join two subsets together

JoinProductReview = JOIN ReviewMerge BY asin, ProductMerge BY asin;

-- Create a new column to calculate the average rating per product

ratingAverage = FOREACH productID GENERATE 

-- Order by average rating

JustProducts = ORDER JustProducts BY ratingAverage DESC;

