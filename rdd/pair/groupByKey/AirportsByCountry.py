import sys
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setAppName('airports-by-country').setMaster('local')

    sc = SparkContext(conf=conf)

    lines = sc.textFile('in/airports.text')

    countryAirportPairRDD = lines.map(lambda airport: (Utils.COMMA_DELIMITER.split(airport)[3],Utils.COMMA_DELIMITER.split(airport)[1]))

    airportsByCountry = countryAirportPairRDD.groupByKey()

    for country, airportName in airportsByCountry.collectAsMap().items():
        print(f'{country} : {list(airportName)}')