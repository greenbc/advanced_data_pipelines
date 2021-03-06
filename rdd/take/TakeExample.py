from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('take').setMaster('local[*]')

    sc = SparkContext(conf=conf)

    inputWords = ['spark','hadoop','spark','hive','pig','cassandra','hadoop']

    wordRDD = sc.parallelize(inputWords)

    words = wordRDD.take(3) # Only take the first 3 results

    for word in words:
        print(word)