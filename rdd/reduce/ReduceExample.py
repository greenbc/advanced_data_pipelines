from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduce').setMaster('local[*]')

    sc = SparkContext(conf=conf)

    inputIntegers = [1,2,3,4,5]

    integerRDD = sc.parallelize(inputIntegers)

    product = integerRDD.reduce(lambda x,y : x * y)

    print(f'The product after reducing is: {product}')