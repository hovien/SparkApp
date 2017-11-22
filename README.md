#Twitter streaming with Spark

The purpose of this project was to stream tweets and find the relevant ones to gather market intelligence. Spark has Twitter compatibility but only in the Scala API, so a workaround was needed to use it with Python. The TwythonReader.py file streames and forwards tweets to Spark Streaming. The code was based on this example: http://www.awesomestats.in/spark-twitter-stream/. The filtering is very simplistic at this point, a keyword search done by regular expressions. The tweets that have one of the keywords are output into a MySQL database.

#Code example

The following code receives the tweets from the socket and filters them by three different topics. Keywords are in two languages, German and English. Non-ASCII characters are included by their Unicode code, as in Öl -> \u0076l. The foreachRDD() function is used for outputting into the MySQL database. 

```python
tweets = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
tweets_oil = tweets.filter(lambda tweet: re.search(u' oil | \u0076l | fuel+ ', tweet))
tweets_oil.foreachRDD(lambda tweetRDD: write_oil_to_MySQL(tweetRDD, jdbcUrl, connProps))
tweets_energy = tweets.filter(lambda tweet: re.search(u' energy | energie | fuel+ ', tweet))
tweets_energy.foreachRDD(lambda tweetRDD: write_energy_to_MySQL(tweetRDD, jdbcUrl, connProps))
tweets_construction = tweets.filter(lambda tweet: re.search(u' construction | Konstruktion | building+ | Bau', tweet))
tweets_construction.foreachRDD(lambda tweetRDD: write_construction_to_MySQL(tweetRDD, jdbcUrl, connProps))
```
This snippet contains the function from within the foreachRDD(). The write_oil_to_MySQL() fuction has three arguments: first, the data flow of RDDs, which are in JSON format, and then the data to be able to establish a database connection. The RDDs need to be transformed into DataFrames to be able to write them in a relational database. Afterwards they are written into MySQL with the jdbc connector.

```python
def write_oil_to_MySQL(json_in, jdbcUrl, connectionProperties):
    tweetsDF = spark.read.json(json_in)
    tweetsDF.write.jdbc(url=jdbcUrl, table="oil", properties=connectionProperties, mode="append")
```
#Installation and use

The scripts can be cloned from this repository.

To use the Twython script a twitter account and keys for the OAuth2 authentication are needed.

The jdbc connector of MySQL has to be installed and the path to the jar files have to be included in the launch command of the spark app.

The application is dependent on a MySQL database where the output can be saved. This database needs to have corresponding tables for each topic that is filtered. 

#Issues

The connections are established with the database everytime a batch arrives instead of only when a write is needed, so it is very inefficient.  
