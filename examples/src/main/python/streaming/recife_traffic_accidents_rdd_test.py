from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    sc = SparkContext(appName='PythonStreamingRecifeTrafficAccidents')
    ssc = StreamingContext(sc, 7)

    lines = ssc.textFileStream('file://Users/ibarros/Downloads/linhas-aleatorias-acidentes2018.csv')
    counts = lines.map(lambda line: line.split('|'))\
                  .filter(lambda values: str(values[13]) != '')\
                  .map(lambda values: (str(values[4]), int(values[13])))\
                  .reduceByKey(lambda v1, v2: v1+v2)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
