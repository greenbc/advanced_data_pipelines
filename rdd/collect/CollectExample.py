from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('collect').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # Create a list of words (basic python list)

    inputWords = ['spark', 'hadoop', 'spark', 'hive', 'pig', 'cassandra', 'hadoop']

    wordRDD = sc.parallelize(inputWords)

    words = wordRDD.collect()

    for word in words:
        print(word)