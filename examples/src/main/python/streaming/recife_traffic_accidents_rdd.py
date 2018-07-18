from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: network_wordcount_tdc.py <hostname> <port>', file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName='PythonStreamingRecifeTrafficAccidents')
    ssc = StreamingContext(sc, 7)

    host_name = sys.argv[1]
    port = sys.argv[2]
    lines = ssc.socketTextStream(host_name, int(port))
    counts = lines.map(lambda line: line.split('|'))\
                  .filter(lambda values: str(values[13]) != '')\
                  .map(lambda values: (str(values[4]), int(values[13])))\
                  .reduceByKey(lambda v1, v2: v1+v2)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
