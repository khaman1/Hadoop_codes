ratings = LOAD '/user/maria_dev/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/u.item' Using PigStorage('|') AS
		(movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
        
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

fiveStarMoviesWithData = JOIN fiveStarMovies by movieID, nameLookup by movieID;

-- SORT the LIST by DESCEND ORDER

oldestFiveStarMovies = ORDER fiveStarMoviesWithData by avgRating DESC, releaseTime DESC;

dump oldestFiveStarMovies;