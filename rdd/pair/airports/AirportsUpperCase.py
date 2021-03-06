import sys
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('airports').setMaster('local')

    sc = SparkContext(conf=conf)

    airportsRDD = sc.textFile('in/airports.text')

    airportPairRDD = airportsRDD.map(lambda line: (Utils.COMMA_DELIMITER.split(line)[1], Utils.COMMA_DELIMITER.split(line)[3]) )

    uppercase = airportPairRDD.mapValues(lambda countryName: countryName.upper())

    uppercase.saveAsTextFile('out/airports_uppercase')