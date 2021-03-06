from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduce-word-counts').setMaster('local')

    sc = SparkContext(conf=conf)

    lines = sc.textFile('in/word_count.text')

    wordRDD = lines.flatMap(lambda line: line.split(' '))

    wordPairRDD = wordRDD.map(lambda word: (word, 1))

    wordCounts = wordPairRDD.reduceByKey(lambda x, y: x + y)

    for word, count in wordCounts.collect():
        print(f'{word} : {count}')