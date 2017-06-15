from pyspark import SparkConf, SparkContext
import os

os.system('export SPARK_MAJOR_VERSION=1')

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
def loadMovieNames(ItemList):
    movieNames = {}
    with open(ItemList) as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    DataList = "hdfs:///user/maria_dev/u.data"
    ItemList = "./../../u.item"
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames(ItemList)

    results = sc.textFile(DataList)\
                .map(parseInput) \
                .reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1])) \
                .filter(lambda x: x[1][1]>10) \
                .mapValues(lambda totalAndCount: (totalAndCount[0]/totalAndCount[1], totalAndCount[1])) \
                .sortBy(lambda x: x[1][0]) \
                .take(20) 

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1][0], result[1][1])
