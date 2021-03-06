import sys
sys.path.insert(0, '.')
# from commons.Utils import Utils
from pyspark import SparkContext, SparkConf
import re

COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line):
    splits = COMMA_DELIMITER.split(line)
    return '{} : {}'.format(splits[1],splits[2])

if __name__ == '__main__':
    conf = SparkConf().setAppName('airports').setMaster('local[2]')
    sc = SparkContext(conf=conf)

    local_file = 'in/airports.text'
    gs_file = 'gs://data-pipeline-example-bg-21/in/airports.text'

    local_output_file = 'out/airports_in_usa.text'
    gs_out_file = 'gs://data-pipeline-example-bg-21'
    
    # Read in airports.text file
    airports = sc.textFile(gs_file)
    airportsInUs = airports.filter(lambda line: COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    airportsNameAndCityNames = airportsInUs.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile('{}/out/airports_in_usa'.format(gs_out_file))