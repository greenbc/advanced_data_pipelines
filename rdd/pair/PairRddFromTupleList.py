from pyspark import SparkConf, SparkContext

"""
    def coalesce(self,numPartitions,shuffle=False)
        Return a new RDD that is reduced into 'numPartitions' partitions
"""

if __name__ == '__main__':
    conf = SparkConf().setAppName('create-with-tuples').setMaster('local')

    sc = SparkContext(conf=conf)
    
    tuples = [('Lily',23),('Jack',29),('Mary',29),('James',8)]

    pairRDD = sc.parallelize(tuples)

    pairRDD.coalesce(1).saveAsTextFile('out/pair_rdd_from_tuple_list')