from pyspark import SparkConf, SparkContext
import numpy
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

if __name__ == "__main__":
    #DataList = "hdfs:///user/maria_dev/u.data"
    ItemList = "./../u.item"
    # The main script - create our SparkContext
    #conf = SparkConf().setAppName("WorstMovies")
    # sc = SparkContext(conf = conf)
    
    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames(ItemList)
    
    max=0;
    TmpMovie='';
    for i in range (1,len(movieNames)):
        if max < len(movieNames[i]):
            max = len(movieNames[i])
            TmpMovie = movieNames[i];
            
    print TmpMovie