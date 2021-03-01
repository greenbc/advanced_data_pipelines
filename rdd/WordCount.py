from pyspark import SparkContext

if __name__ == '__main__':

    gcs_uri = 'gs://joel-pyspark-cloud-demo/in/word_count.text'
    local_uri = 'in/word_count.text'

    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel('ERROR')

    lines = sc.textFile(local_uri) # Read in basic text file with spark

    words = lines.flatMap(lambda line: line.split(' ')) # Returns an RDD

    wordCounts = words.countByValue() #Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

    for word, count in wordCounts.items():
        print(word, count)