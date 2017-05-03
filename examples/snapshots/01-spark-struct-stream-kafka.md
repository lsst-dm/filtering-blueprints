
# Spark Structured Streaming from Kafka

Example of using Spark to connect to Kafka and using Spark Structured Streaming to process a Kafka stream of Python alerts in non-Avro string format.

## Notes

Useful references.

https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html

https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html

## Prep environment

Need some packages to talk to Kafka.


```python
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'

```


```python
from ast import literal_eval
```

## Start a Kafka stream for Spark to subscribe

With lsst-dm/alert_stream, in an external shell:

Send some alerts so stream exists to connect to:

docker run -it       --network=alertstream_default       alert_stream python bin/sendAlertStream.py my-stream 10 --no-stamps --encode-off

## Set up Spark session and connection to Kakfa


```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()
    
# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts    
dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "my-stream") \
  .option("startingOffsets", "earliest") \
  .load()
  
```


```python
ds = dsraw.selectExpr("CAST(value AS STRING)")
```


```python
print(type(dsraw))
print(type(ds))
```

    <class 'pyspark.sql.dataframe.DataFrame'>
    <class 'pyspark.sql.dataframe.DataFrame'>


dsraw is the raw data stream, in "kafka" format.

ds pulls out the "value" from "kafka" format, the actual alert data.

## Create output for Spark Structured Streaming

Queries are new sql dataframe streams and can be written to disk or saved to memory for followup sql operations.
Below they are saved to memory with queryNames that can be treated as tables by spark.sql.



```python
rawQuery = dsraw \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .start()
```


```python
alertQuery = ds \
        .writeStream \
        .queryName("qalerts")\
        .format("memory")\
        .start()
```

Send some alerts to get queries to recognize activity:

docker run -it       --network=alertstream_default       alert_stream python bin/sendAlertStream.py my-stream 10 --no-stamps --encode-off

Use sql operations on the named in-memory query tables.


```python
raw = spark.sql("select * from qraw")
raw.show()
```

    +----+--------------------+---------+---------+------+--------------------+-------------+
    | key|               value|    topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+---------+---------+------+--------------------+-------------+
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     0|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     1|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     2|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     3|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     4|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     5|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     6|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     7|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     8|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|     9|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    10|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    11|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    12|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    13|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    14|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    15|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    16|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    17|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    18|1969-12-31 23:59:...|            0|
    |null|[7B 27 61 6C 65 7...|my-stream|        0|    19|1969-12-31 23:59:...|            0|
    +----+--------------------+---------+---------+------+--------------------+-------------+
    


Because this stream is format="kafka," the schema of the table reflects the data structure of Kafka streams, not of our data content, which is stored in "value."


```python
raw.printSchema()
```

    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)
    


Try selecting from the stream that has cast the kafka "value" to strings.


```python
alerts = spark.sql("select * from qalerts")
alerts.show()
```

    +--------------------+
    |               value|
    +--------------------+
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    |{'alertId': 12313...|
    +--------------------+
    


The alert data has no known schema, only str.


```python
alerts.printSchema()
```

    root
     |-- value: string (nullable = true)
    



```python
type(alerts)
```




    pyspark.sql.dataframe.DataFrame



## Convert the values in the sql dataframe to RDD of dicts

We can use the RDDs to convert data to a better structure for filtering.


```python
rddAlertsRdd = alerts.rdd.map(lambda alert: literal_eval(alert['value']))
```


```python
rddAlertsRdd
```




    PythonRDD[24] at RDD at PythonRDD.scala:48




```python
rddAlerts = rddAlertsRdd.collect()
```


```python
type(rddAlerts)
```




    list




```python
rddAlerts[0]
```




    {'alertId': 1231321321,
     'diaObject': {'decl': 0.126243049656,
      'diaObjectId': 281323062375219201,
      'flags': 0,
      'parallax': 2.124124,
      'pmDecl': 0.00014,
      'pmParallaxChi2': 0.00013,
      'pmParallaxLnL': 0.00013,
      'pmParallaxNdata': 1214,
      'pmRa': 0.00013,
      'pm_parallax_Cov': {'parallaxSigma': 0.00013,
       'pmDeclSigma': 0.00013,
       'pmDecl_parallax_Cov': 0.00013,
       'pmRaSigma': 0.00013,
       'pmRa_parallax_Cov': 0.00013,
       'pmRa_pmDecl_Cov': 0.00013},
      'ra': 351.570546978,
      'ra_decl_Cov': {'declSigma': 0.00028,
       'raSigma': 0.00028,
       'ra_decl_Cov': 0.00029},
      'radecTai': 1480360995},
     'diaObjectL2': {'decl': 0.126243049656,
      'diaObjectId': 281323062375219201,
      'flags': 0,
      'parallax': 2.124124,
      'pmDecl': 0.00014,
      'pmParallaxChi2': 0.00013,
      'pmParallaxLnL': 0.00013,
      'pmParallaxNdata': 1214,
      'pmRa': 0.00013,
      'pm_parallax_Cov': {'parallaxSigma': 0.00013,
       'pmDeclSigma': 0.00013,
       'pmDecl_parallax_Cov': 0.00013,
       'pmRaSigma': 0.00013,
       'pmRa_parallax_Cov': 0.00013,
       'pmRa_pmDecl_Cov': 0.00013},
      'ra': 351.570546978,
      'ra_decl_Cov': {'declSigma': 0.00028,
       'raSigma': 0.00028,
       'ra_decl_Cov': 0.00029},
      'radecTai': 1480360995},
     'diaSource': {'ccdVisitId': 111111,
      'decl': 0.126243049656,
      'diaSourceId': 281323062375219200,
      'filterName': 'r',
      'flags': 0,
      'midPointTai': 1480360995,
      'psFlux': 1241.0,
      'ra': 351.570546978,
      'ra_decl_Cov': {'declSigma': 0.00028,
       'raSigma': 0.00028,
       'ra_decl_Cov': 0.00029},
      'snr': 41.1,
      'x': 112.1,
      'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
      'y': 121.1},
     'diaSourcesL2': [{'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219198,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1},
      {'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219199,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1}],
     'l1dbId': 222222222,
     'prv_diaSources': [{'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219198,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1},
      {'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219199,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1}],
     'ssObject': {'MOID1': 3141.0,
      'MOID2': 23432.423,
      'arc': 2.124124,
      'flags': 0,
      'moidLon1': 2143.213,
      'moidLon2': 3142.23123,
      'oe': {'M': 131.1241,
       'aop': 344243.3,
       'e': 636.121,
       'epoch': 134141,
       'i': 5436.2344,
       'lan': 54325.34,
       'q': 6654.14},
      'orbFitChi2': 1341421.2414,
      'orbFitLnL': 1343141.0341,
      'orbFitNdata': 1214,
      'ssObjectId': 5364546,
      'uG1': 32131.312,
      'uG1Err': 31232.2132,
      'uG2': 231.2313,
      'uG2Err': 23132.231,
      'uH': 13231.231,
      'uHErr': 13213.213}}



Do filtering as list comprehension.


```python
ra_all = [alert for alert in rddAlerts \
                 if alert['diaSource']['ra'] > 350 ]
```


```python
print(len(ra_all))
```

    20



```python
ra_all[0:2]
```




    [{'alertId': 1231321321,
      'diaObject': {'decl': 0.126243049656,
       'diaObjectId': 281323062375219201,
       'flags': 0,
       'parallax': 2.124124,
       'pmDecl': 0.00014,
       'pmParallaxChi2': 0.00013,
       'pmParallaxLnL': 0.00013,
       'pmParallaxNdata': 1214,
       'pmRa': 0.00013,
       'pm_parallax_Cov': {'parallaxSigma': 0.00013,
        'pmDeclSigma': 0.00013,
        'pmDecl_parallax_Cov': 0.00013,
        'pmRaSigma': 0.00013,
        'pmRa_parallax_Cov': 0.00013,
        'pmRa_pmDecl_Cov': 0.00013},
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'radecTai': 1480360995},
      'diaObjectL2': {'decl': 0.126243049656,
       'diaObjectId': 281323062375219201,
       'flags': 0,
       'parallax': 2.124124,
       'pmDecl': 0.00014,
       'pmParallaxChi2': 0.00013,
       'pmParallaxLnL': 0.00013,
       'pmParallaxNdata': 1214,
       'pmRa': 0.00013,
       'pm_parallax_Cov': {'parallaxSigma': 0.00013,
        'pmDeclSigma': 0.00013,
        'pmDecl_parallax_Cov': 0.00013,
        'pmRaSigma': 0.00013,
        'pmRa_parallax_Cov': 0.00013,
        'pmRa_pmDecl_Cov': 0.00013},
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'radecTai': 1480360995},
      'diaSource': {'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219200,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1},
      'diaSourcesL2': [{'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219198,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1},
       {'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219199,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1}],
      'l1dbId': 222222222,
      'prv_diaSources': [{'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219198,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1},
       {'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219199,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1}],
      'ssObject': {'MOID1': 3141.0,
       'MOID2': 23432.423,
       'arc': 2.124124,
       'flags': 0,
       'moidLon1': 2143.213,
       'moidLon2': 3142.23123,
       'oe': {'M': 131.1241,
        'aop': 344243.3,
        'e': 636.121,
        'epoch': 134141,
        'i': 5436.2344,
        'lan': 54325.34,
        'q': 6654.14},
       'orbFitChi2': 1341421.2414,
       'orbFitLnL': 1343141.0341,
       'orbFitNdata': 1214,
       'ssObjectId': 5364546,
       'uG1': 32131.312,
       'uG1Err': 31232.2132,
       'uG2': 231.2313,
       'uG2Err': 23132.231,
       'uH': 13231.231,
       'uHErr': 13213.213}},
     {'alertId': 1231321321,
      'diaObject': {'decl': 0.126243049656,
       'diaObjectId': 281323062375219201,
       'flags': 0,
       'parallax': 2.124124,
       'pmDecl': 0.00014,
       'pmParallaxChi2': 0.00013,
       'pmParallaxLnL': 0.00013,
       'pmParallaxNdata': 1214,
       'pmRa': 0.00013,
       'pm_parallax_Cov': {'parallaxSigma': 0.00013,
        'pmDeclSigma': 0.00013,
        'pmDecl_parallax_Cov': 0.00013,
        'pmRaSigma': 0.00013,
        'pmRa_parallax_Cov': 0.00013,
        'pmRa_pmDecl_Cov': 0.00013},
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'radecTai': 1480360995},
      'diaObjectL2': {'decl': 0.126243049656,
       'diaObjectId': 281323062375219201,
       'flags': 0,
       'parallax': 2.124124,
       'pmDecl': 0.00014,
       'pmParallaxChi2': 0.00013,
       'pmParallaxLnL': 0.00013,
       'pmParallaxNdata': 1214,
       'pmRa': 0.00013,
       'pm_parallax_Cov': {'parallaxSigma': 0.00013,
        'pmDeclSigma': 0.00013,
        'pmDecl_parallax_Cov': 0.00013,
        'pmRaSigma': 0.00013,
        'pmRa_parallax_Cov': 0.00013,
        'pmRa_pmDecl_Cov': 0.00013},
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'radecTai': 1480360995},
      'diaSource': {'ccdVisitId': 111111,
       'decl': 0.126243049656,
       'diaSourceId': 281323062375219200,
       'filterName': 'r',
       'flags': 0,
       'midPointTai': 1480360995,
       'psFlux': 1241.0,
       'ra': 351.570546978,
       'ra_decl_Cov': {'declSigma': 0.00028,
        'raSigma': 0.00028,
        'ra_decl_Cov': 0.00029},
       'snr': 41.1,
       'x': 112.1,
       'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
       'y': 121.1},
      'diaSourcesL2': [{'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219198,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1},
       {'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219199,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1}],
      'l1dbId': 222222222,
      'prv_diaSources': [{'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219198,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1},
       {'ccdVisitId': 111111,
        'decl': 0.126243049656,
        'diaSourceId': 281323062375219199,
        'filterName': 'r',
        'flags': 0,
        'midPointTai': 1480360995,
        'psFlux': 1241.0,
        'ra': 351.570546978,
        'ra_decl_Cov': {'declSigma': 0.00028,
         'raSigma': 0.00028,
         'ra_decl_Cov': 0.00029},
        'snr': 41.1,
        'x': 112.1,
        'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
        'y': 121.1}],
      'ssObject': {'MOID1': 3141.0,
       'MOID2': 23432.423,
       'arc': 2.124124,
       'flags': 0,
       'moidLon1': 2143.213,
       'moidLon2': 3142.23123,
       'oe': {'M': 131.1241,
        'aop': 344243.3,
        'e': 636.121,
        'epoch': 134141,
        'i': 5436.2344,
        'lan': 54325.34,
        'q': 6654.14},
       'orbFitChi2': 1341421.2414,
       'orbFitLnL': 1343141.0341,
       'orbFitNdata': 1214,
       'ssObjectId': 5364546,
       'uG1': 32131.312,
       'uG1Err': 31232.2132,
       'uG2': 231.2313,
       'uG2Err': 23132.231,
       'uH': 13231.231,
       'uHErr': 13213.213}}]




```python
ra_empty = [alert for alert in rddAlerts \
                 if alert['diaSource']['ra'] < 350 ]
```


```python
print(len(ra_empty))
```

    0


## Converting from RDD of dicts to a dataframe is not a good idea

The schema is inferred incorrectly, and data can be lost, shown below.  So, don't try to do filtering with sql dataframes.


```python
df = rddAlertsRdd.toDF()
```

    /usr/local/spark/python/pyspark/sql/session.py:336: UserWarning: Using RDD of dict to inferSchema is deprecated. Use pyspark.sql.Row instead
      warnings.warn("Using RDD of dict to inferSchema is deprecated. "



```python
type(df)
```




    pyspark.sql.dataframe.DataFrame




```python
df.show()
```

    +----------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    |   alertId|           diaObject|         diaObjectL2|           diaSource|        diaSourcesL2|   l1dbId|      prv_diaSources|            ssObject|
    +----------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    |1231321321|Map(decl -> 0.126...|Map(decl -> 0.126...|Map(x -> null, cc...|[Map(x -> null, c...|222222222|[Map(x -> null, c...|Map(orbFitNdata -...|
    +----------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
    



```python
alertIds = df.select('alertId')
```


```python
alertIds.show()
```

    +----------+
    |   alertId|
    +----------+
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    |1231321321|
    +----------+
    


Filtering _appears_ to be working above, for the data that is not lost.  Below shows NULLs where data has been lost.


```python
diaSources = df.select('diaSource').where('diaSource.diaSourceId is not NULL')
```


```python
diaSources_empty = df.select('diaSource').where('diaSource.diaSourceId is NULL')
```


```python
type(diaSources)
```




    pyspark.sql.dataframe.DataFrame




```python
diaSources.show()
```

    +---------+
    |diaSource|
    +---------+
    +---------+
    



```python
diaSources_empty.show()
```

    +--------------------+
    |           diaSource|
    +--------------------+
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    +--------------------+
    


Take a closer look at diaSources_empty with a pandas dataframe.


```python
pd = diaSources_empty.toPandas()
```


```python
pd
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>diaSource</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>14</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>17</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>18</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
    <tr>
      <th>19</th>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
    </tr>
  </tbody>
</table>
</div>




```python
type(pd['diaSource'][0])
```




    dict




```python
pd['diaSource'][0]
```




    {'ccdVisitId': None,
     'decl': None,
     'diaSourceId': None,
     'filterName': None,
     'flags': None,
     'midPointTai': None,
     'psFlux': None,
     'ra': None,
     'ra_decl_Cov': {'declSigma': 0.00028,
      'raSigma': 0.00028,
      'ra_decl_Cov': 0.00029},
     'snr': None,
     'x': None,
     'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
     'y': None}



Some data has been misinterpreted, shown by the "None"s above.

What if we try from the pre-pandas sql dataframe?  Checking to see if it was the pandas conversion that lost data.


```python
diaSources_empty.filter('diaSource.filterName is NULL').show()
```

    +--------------------+
    |           diaSource|
    +--------------------+
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    |Map(x -> null, cc...|
    +--------------------+
    


No, shown above, data was lost before the pandas conversion.  Don't do RDD.toDF() when RDD is dicts.

## Original sql.dataframe to pandas is ok


```python
pdAlerts = alerts.toPandas()
```


```python
pdAlerts['value'][0]
```




    "{'alertId': 1231321321, 'l1dbId': 222222222, 'diaSource': {'diaSourceId': 281323062375219200, 'ccdVisitId': 111111, 'midPointTai': 1480360995, 'filterName': 'r', 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'x': 112.1, 'y': 121.1, 'x_y_Cov': {'xSigma': 1.2, 'ySigma': 1.1, 'x_y_Cov': 1.2}, 'snr': 41.1, 'psFlux': 1241.0, 'flags': 0}, 'prv_diaSources': [{'diaSourceId': 281323062375219198, 'ccdVisitId': 111111, 'midPointTai': 1480360995, 'filterName': 'r', 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'x': 112.1, 'y': 121.1, 'x_y_Cov': {'xSigma': 1.2, 'ySigma': 1.1, 'x_y_Cov': 1.2}, 'snr': 41.1, 'psFlux': 1241.0, 'flags': 0}, {'diaSourceId': 281323062375219199, 'ccdVisitId': 111111, 'midPointTai': 1480360995, 'filterName': 'r', 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'x': 112.1, 'y': 121.1, 'x_y_Cov': {'xSigma': 1.2, 'ySigma': 1.1, 'x_y_Cov': 1.2}, 'snr': 41.1, 'psFlux': 1241.0, 'flags': 0}], 'diaObject': {'diaObjectId': 281323062375219201, 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'radecTai': 1480360995, 'pmRa': 0.00013, 'pmDecl': 0.00014, 'parallax': 2.124124, 'pm_parallax_Cov': {'pmRaSigma': 0.00013, 'pmDeclSigma': 0.00013, 'parallaxSigma': 0.00013, 'pmRa_pmDecl_Cov': 0.00013, 'pmRa_parallax_Cov': 0.00013, 'pmDecl_parallax_Cov': 0.00013}, 'pmParallaxLnL': 0.00013, 'pmParallaxChi2': 0.00013, 'pmParallaxNdata': 1214, 'flags': 0}, 'ssObject': {'ssObjectId': 5364546, 'oe': {'q': 6654.14, 'e': 636.121, 'i': 5436.2344, 'lan': 54325.34, 'aop': 344243.3, 'M': 131.1241, 'epoch': 134141}, 'arc': 2.124124, 'orbFitLnL': 1343141.0341, 'orbFitChi2': 1341421.2414, 'orbFitNdata': 1214, 'MOID1': 3141.0, 'MOID2': 23432.423, 'moidLon1': 2143.213, 'moidLon2': 3142.23123, 'uH': 13231.231, 'uHErr': 13213.213, 'uG1': 32131.312, 'uG1Err': 31232.2132, 'uG2': 231.2313, 'uG2Err': 23132.231, 'flags': 0}, 'diaObjectL2': {'diaObjectId': 281323062375219201, 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'radecTai': 1480360995, 'pmRa': 0.00013, 'pmDecl': 0.00014, 'parallax': 2.124124, 'pm_parallax_Cov': {'pmRaSigma': 0.00013, 'pmDeclSigma': 0.00013, 'parallaxSigma': 0.00013, 'pmRa_pmDecl_Cov': 0.00013, 'pmRa_parallax_Cov': 0.00013, 'pmDecl_parallax_Cov': 0.00013}, 'pmParallaxLnL': 0.00013, 'pmParallaxChi2': 0.00013, 'pmParallaxNdata': 1214, 'flags': 0}, 'diaSourcesL2': [{'diaSourceId': 281323062375219198, 'ccdVisitId': 111111, 'midPointTai': 1480360995, 'filterName': 'r', 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'x': 112.1, 'y': 121.1, 'x_y_Cov': {'xSigma': 1.2, 'ySigma': 1.1, 'x_y_Cov': 1.2}, 'snr': 41.1, 'psFlux': 1241.0, 'flags': 0}, {'diaSourceId': 281323062375219199, 'ccdVisitId': 111111, 'midPointTai': 1480360995, 'filterName': 'r', 'ra': 351.570546978, 'decl': 0.126243049656, 'ra_decl_Cov': {'raSigma': 0.00028, 'declSigma': 0.00028, 'ra_decl_Cov': 0.00029}, 'x': 112.1, 'y': 121.1, 'x_y_Cov': {'xSigma': 1.2, 'ySigma': 1.1, 'x_y_Cov': 1.2}, 'snr': 41.1, 'psFlux': 1241.0, 'flags': 0}]}"




```python
type(pdAlerts['value'][0])
```




    str




```python
pdAlertsSeries = pdAlerts['value'].map(literal_eval)
```


```python
type(pdAlertsSeries)
```




    pandas.core.series.Series




```python
pdAlertsSeries[0:3]
```




    0    {'prv_diaSources': [{'ra_decl_Cov': {'raSigma'...
    1    {'prv_diaSources': [{'ra_decl_Cov': {'raSigma'...
    2    {'prv_diaSources': [{'ra_decl_Cov': {'raSigma'...
    Name: value, dtype: object




```python
type(pdAlertsSeries[0])
```




    dict




```python
import pandas
pdAlertsDf = pandas.DataFrame(list(pdAlertsSeries))
```


```python
type(pdAlertsDf)
```




    pandas.core.frame.DataFrame




```python
pdAlertsDf.head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>alertId</th>
      <th>diaObject</th>
      <th>diaObjectL2</th>
      <th>diaSource</th>
      <th>diaSourcesL2</th>
      <th>l1dbId</th>
      <th>prv_diaSources</th>
      <th>ssObject</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1231321321</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>222222222</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>{'uG2': 231.2313, 'arc': 2.124124, 'uG2Err': 2...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1231321321</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>222222222</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>{'uG2': 231.2313, 'arc': 2.124124, 'uG2Err': 2...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1231321321</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>222222222</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>{'uG2': 231.2313, 'arc': 2.124124, 'uG2Err': 2...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1231321321</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>222222222</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>{'uG2': 231.2313, 'arc': 2.124124, 'uG2Err': 2...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1231321321</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'pmRa': 0.00013, 'pmParallaxNdata': 1214, 'pm...</td>
      <td>{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_...</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>222222222</td>
      <td>[{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl...</td>
      <td>{'uG2': 231.2313, 'arc': 2.124124, 'uG2Err': 2...</td>
    </tr>
  </tbody>
</table>
</div>




```python
pdAlertsDf['diaSource'][0]
```




    {'ccdVisitId': 111111,
     'decl': 0.126243049656,
     'diaSourceId': 281323062375219200,
     'filterName': 'r',
     'flags': 0,
     'midPointTai': 1480360995,
     'psFlux': 1241.0,
     'ra': 351.570546978,
     'ra_decl_Cov': {'declSigma': 0.00028,
      'raSigma': 0.00028,
      'ra_decl_Cov': 0.00029},
     'snr': 41.1,
     'x': 112.1,
     'x_y_Cov': {'xSigma': 1.2, 'x_y_Cov': 1.2, 'ySigma': 1.1},
     'y': 121.1}




```python
type(pdAlertsDf['diaSource'][0])
```




    dict




```python
type(pdAlertsDf['diaSource'][0]['ra_decl_Cov'])
```




    dict




```python
pdAlertsDf['diaSource'][0]['ra_decl_Cov']
```




    {'declSigma': 0.00028, 'raSigma': 0.00028, 'ra_decl_Cov': 0.00029}



Nested dicts look like they have survived, when creating a pandas dataframe from a list from a spark series.

But pyspark.sql.dataframe creation can infer data structure incorrectly, if the data does not have a schema.


```python
sparkdf = spark.createDataFrame([list(pdAlertsSeries)])
```


```python
type(sparkdf)
```




    pyspark.sql.dataframe.DataFrame




```python
sparkdf.collect()
```




    [Row(_1={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _2={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _3={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _4={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _5={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _6={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _7={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _8={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _9={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _10={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _11={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _12={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _13={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _14={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _15={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _16={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _17={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _18={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _19={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None}, _20={'prv_diaSources': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'alertId': None, 'diaObject': None, 'l1dbId': None, 'diaSourcesL2': [{'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}, {'ra_decl_Cov': {'raSigma': 0.00028, 'ra_decl_Cov': 0.00029, 'declSigma': 0.00028}, 'x_y_Cov': {'ySigma': 1.1, 'x_y_Cov': 1.2, 'xSigma': 1.2}, 'diaSourceId': None, 'x': None, 'decl': None, 'snr': None, 'psFlux': None, 'y': None, 'filterName': None, 'flags': None, 'ra': None, 'midPointTai': None, 'ccdVisitId': None}], 'ssObject': None, 'diaObjectL2': None, 'diaSource': None})]



## Summary

Using Spark Structured Streaming with a Kafka formatted stream and Kafka stream values of alerts that are unstructured (non-Avro, strings) is possible for filtering, but really a roundabout solution, if you do either of the following:


### Filter using list comprehension
1. Read a structured stream from Kafka
2. Convert "values" to strings.
3. Construct a pyspark.sql.df selecting all of the values.
4. Use rdd.map to do literal_eval on the strings to convert to rdds of dicts.
5. Collect the rdds into a list of dicts.
6. Filter on the list of dicts using list comprehension.

But, issues can unknowingly arise if after step 4 you try and convert to pyspark.sql.dataframes to do filtering (using RDD.toDF() method).


### Filter using pandas
1. Read a structured stream from Kafka
2. Convert "values" to strings.
3. Construct a pyspark.sql.df selecting all of the values.
4. Convert the above pyspark.sql.df toPandas().
5. Column map "values" to do literal_eval on the strings to convert to a pandas series of dicts.
6. Create a pandas dataframe from list(above series) and filter using pandas.

But, again, issues can unknowingly arise if after step 5 you try and create a pyspark.sql.dataframe from the series of dicts to do filtering with pyspark.sql.dataframes.

The value of using Spark Structured Streaming is primarily in the ability to use pyspark.sql on structured data, so for this example, using Spark Structured Streaming isn't particulary useful.

