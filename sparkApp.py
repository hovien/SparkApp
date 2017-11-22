from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def connect_to_MySQL():
    hostname = "165.227.158.183"
    dbname = "Tweets"
    connectionProperties = {
        "user" : "streaming",
        "password" : "S1p9a0r4k87at$ql"
    }

    jdbcUrl = "jdbc:mysql://{0}/{1}".format(hostname, dbname)
    return jdbcUrl, connectionProperties

def write_oil_to_MySQL(json_in, jdbcUrl, connectionProperties):
    tweetsDF = spark.read.json(json_in)
    tweetsDF.write.jdbc(url=jdbcUrl, table="oil", properties=connectionProperties, mode="append")
def write_energy_to_MySQL(json_in, jdbcUrl, connectionProperties):
    tweetsDF = spark.read.json(json_in)
    tweetsDF.write.jdbc(url=jdbcUrl, table="energy", properties=connectionProperties, mode="append")
def write_construction_to_MySQL(json_in, jdbcUrl, connectionProperties):
    tweetsDF = spark.read.json(json_in)
    tweetsDF.write.jdbc(url=jdbcUrl, table="construction", properties=connectionProperties, mode="append")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    jdbcUrl, connProps = connect_to_MySQL()

    tweets = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

#KEY WORDS CAN BE CHANGED INSIDE THE UNICODE STRINGS IN THE REGEX SEARCH
#Text has to be Unicode string, not byte string
#to prevent encoing errors.
#Results are written in the corresponding MySQL table,
#so to track a new topic a new table has to be created in MySQL too.

    tweets_oil = tweets.filter(lambda tweet: re.search(u' oil | \u0076l | fuel+ ', tweet))
    tweets_oil.foreachRDD(lambda tweetRDD: write_oil_to_MySQL(tweetRDD, jdbcUrl, connProps))
    tweets_energy = tweets.filter(lambda tweet: re.search(u' energy | energie | fuel+ ', tweet))
    tweets_energy.foreachRDD(lambda tweetRDD: write_energy_to_MySQL(tweetRDD, jdbcUrl, connProps))
    tweets_construction = tweets.filter(lambda tweet: re.search(u' construction | Konstruktion | building+ | Bau', tweet))
    tweets_construction.foreachRDD(lambda tweetRDD: write_construction_to_MySQL(tweetRDD, jdbcUrl, connProps))

    ssc.start()
    ssc.awaitTermination()
