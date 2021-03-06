import sys
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('airports-not-US').setMaster('local')

    sc = SparkContext(conf=conf)

    airportsRDD = sc.textFile('in/airports.text')

    airportPairRDD = airportsRDD.map(lambda line: (Utils.COMMA_DELIMITER.split(line)[1], Utils.COMMA_DELIMITER.split(line)[3]) )

    airportsNotInUSA = airportPairRDD.filter(lambda Value: Value[1] != "\"United States\"")

    airportsNotInUSA.saveAsTextFile('out/airports_not_in_usa')