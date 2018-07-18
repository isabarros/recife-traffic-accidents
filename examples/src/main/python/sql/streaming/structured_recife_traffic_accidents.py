from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_recife_traffic_accidents.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    spark = SparkSession\
        .builder\
        .appName("StructuredRecifeTrafficAccidents")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    # Split the lines into words
    bike_accidents = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, '|')
        ).alias('word')
    )

    # Generate running word count
    bike_accidents_counts = bike_accidents.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = bike_accidents_counts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

    query.awaitTermination()
