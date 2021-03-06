from pyspark import SparkContext, SparkConf

def isNotHeader(line):
    return not (line.startswith('host') and 'bytes' in line)

if __name__ == '__main__':
    conf = SparkConf().setAppName('unionLogs').setMaster('local[*]')

    sc = SparkContext(conf = conf)

    # Create two Log RDDs
    julyFirstLogs = sc.textFile('in/nasa_19950701.tsv')
    augustFirstLogs = sc.textFile('in/nasa_19950801.tsv')

    # Aggregate the log lines together -- with Union

    aggregateLogLines = julyFirstLogs.union(augustFirstLogs)

    cleanLogLines = aggregateLogLines.filter(isNotHeader)

    sample = cleanLogLines.sample(withReplacement=True, fraction=0.1)

    sample.saveAsTextFile('out/sample_nasa_logs.csv')