from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("./../../u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    movies = spark.sparkContext.textFile("hdfs:///user/maria_dev/u.data") \
                  .map(parseInput) # Convert it to a RDD of Row objects with (movieID, rating)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Pull the top 10 results
    topTen = counts.orderBy("count", ascending=False).take(10)

    
    # If we want to retrieve the columns name in topTen, just print it out bytearray
    # print topTen
    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1])

    # Stop the session
    spark.stop()
