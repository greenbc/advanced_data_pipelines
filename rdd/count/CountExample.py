from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('count and countByValue').setMaster('local[*]')

    sc = SparkContext(conf=conf)

    inputWords = ['spark','hadoop','spark','hive','pig','cassandra','hadoop']

    wordRDD = sc.parallelize(inputWords)

    print(f'Count: {wordRDD.count()}')

    wordCountByValue = wordRDD.countByValue()

    print('Counting By Value: ')

    for word, count in wordCountByValue.items():
        print(f'{word} : {count}')