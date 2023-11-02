from pyspark.sql import SparkSession

# Import necessary libraries and modules
from pyspark.sql.functions import explode
import pyspark.sql
import json
import yaml
from pyspark.sql.functions import window, last
import time
from timeloop import Timeloop
from datetime import timedelta
from pyspark.sql.functions import window
from pyspark.sql.types import StructField, StructType, StringType, LongType, FloatType, DateType, ShortType, ArrayType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, size, concat, lit, array
import os
import sys
from pyspark import SparkContext

# Set environment variables for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("AppKafka") \
    .config("spark.mongodb.input.uri", "mongodb://localhost/") \
    .config("spark.mongodb.output.uri", "mongodb://localhost/") \
    .getOrCreate()

# Get the SparkContext
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read data from Kafka topic "mbta_msgs"
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("subscribe", "mbta_msgs").load()

# Define a function to stop a streaming query by its name
def sq(name):
    for s in spark.streams.active:
        if s.name == name:
            s.stop()

# Select the "timestamp" and "value" columns from the Kafka data
valueDf = df.selectExpr('timestamp', 'CAST(value AS STRING)')

# Define the schema for the "second_stage" and "first_stage" data
second_stage = StructType((
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("links", StringType(), True),
    StructField("relationships", StringType(), True)))

first_stage = StructType((
    StructField("event", StringType(), True),
    StructField("data", StringType(), True))

# Parse and rename columns in the Kafka data
firstStageDf = valueDf.select('timestamp', from_json(col("value"), first_stage))\
    .withColumnRenamed('jsontostructs(value)', 'proc')

flatFirstStageDf = firstStageDf.selectExpr('timestamp', 'proc.event', 'proc.data')

# Separate data into "reset," "update," and "remove" stages
resetStageDf = flatFirstStageDf.where(col('event') == 'reset')
updateStageDf = flatFirstStageDf.where(col('event') == 'update')
removeStageDf = flatFirstStageDf.where(col('event') == 'remove')

# Process "reset" stage data
secondStageResetDf = resetStageDf.select(col('timestamp'), col('event'), from_json(col("data"), first_stage_reset))\
    .withColumnRenamed('jsontostructs(data)', 'proc')\
    .withColumn('exploded', explode(col('proc')))\
    .select('timestamp', 'event', 'exploded').drop('data')\
    .withColumnRenamed('exploded', 'proc')

# Process "update" stage data
secondStageUpdateDf = updateStageDf.select(col('timestamp'), col('event'), from_json(col("data"), second_stage))\
    .withColumnRenamed('jsontostructs(data)', 'proc')

# Process "remove" stage data
secondStageRemoveDf = updateStageDf.select(col('timestamp'), col('event'), from_json(col("data"), second_stage))\
    .withColumnRenamed('jsontostructs(data)', 'proc')

# Union the data from different stages
secondStageDf = secondStageResetDf.union(secondStageUpdateDf).union(secondStageRemoveDf)

# Flatten the "second_stage" data
flatSecondStageDf = secondStageDf.selectExpr('timestamp', 'event', 'proc.id as _id', 'proc.type', 'proc.attributes', 'proc.links', 'proc.relationships')

# Define schemas for "third_stage_attr" and "third_stage_rel"
third_stage_attr = StructType((
    StructField("bearing", DoubleType(), True),
    StructField("current_status", StringType(), True),
    StructField("current_stop_sequence", IntegerType(), True),
    StructField("label", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("updated_at", DateType(), True)))

third_stage_rel = StructType((
    StructField("route", StringType(), True),
    StructField("stop", StringType(), True),
    StructField("trip", StringType(), True))

# Process the "third_stage" data
thirdStageDf = flatSecondStageDf.select(col('timestamp'), col('event'), col('_id'), col('type'),
                                       from_json(col('attributes'), third_stage_attr),
                                       from_json(col("relationships"), third_stage_rel))\
    .withColumnRenamed('jsontostructs(attributes)', 'attrs')\
    .withColumnRenamed('jsontostructs(relationships)', 'rels')

# Flatten the "third_stage" data
flatThirdStageDf = thirdStageDf.selectExpr('timestamp', 'event', '_id', 'type', 'attrs.bearing', 'attrs.current_status',
                                           'attrs.current_stop_sequence', 'attrs.label', 'attrs.latitude',
                                           'attrs.longitude', 'attrs.speed', 'attrs.updated_at',
                                           'rels.route', 'rels.stop', 'rels.trip')

# Define schemas for "fourth_stage_data" and "fourth_stage_id"
fourth_stage_data = StructType([
    StructField("data", StringType(), True)])

fourth_stage_id = StructType([
    StructField("id", StringType(), True)])

# Process the "fourth_stage" data
fourthStageDf = flatThirdStageDf.select('*',
                                        from_json(col('route'), fourth_stage_data),
                                        from_json(col('stop'), fourth_stage_data),
                                        from_json(col('trip'), fourth_stage_data))\
    .withColumnRenamed('jsontostructs(route)', 'routeproc')\
    .withColumnRenamed('jsontostructs(stop)', 'stopproc')\
    .withColumnRenamed('jsontostructs(trip)', 'tripproc')\
    .drop('route').drop('stop').drop('trip')

# Flatten the "fourth_stage" data
flatFourthStage = fourthStageDf.select('*',
                                        from_json(col('routeproc.data'), fourth_stage_id),
                                        from_json(col('stopproc.data'), fourth_stage_id),
                                        from_json(col('tripproc.data'), fourth_stage_id))\
    .withColumnRenamed('jsontostructs(routeproc.data)', 'route')\
    .withColumnRenamed('jsontostructs(stopproc.data)', 'stop')\
    .withColumnRenamed('jsontostructs(tripproc.data)', 'trip')\
    .drop('routeproc').drop('stopproc').drop('tripproc')

# Select columns for the final stage
finalFourthStage = flatFourthStage.selectExpr('timestamp', 'event', '_id', 'type', 'bearing',
                                          'current_status', 'current_stop_sequence', 'label',
                                          'latitude', 'longitude', 'speed',
                                          'updated_at', 'route.id as route',
                                          'stop.id as stop', 'trip.id as trip')\
                                  .withColumn('coordinates', array(col("longitude"), col("latitude")))

finalStage = finalFourthStage.select(col('timestamp'), col('event'), col('_id'), col('type'),
                                     col('bearing'),
                                     col('current_status'), col('current_stop_sequence'),
                                     col('label'), col('latitude'), col('longitude'),
                                     col('coordinates'), col('speed'),
                                     col('updated_at'), col('route'),
                                     col('stop'), col('trip'))

# Define a function to write data to MongoDB
def foreach_batch_function(dfb, epoch_id):
    dfb.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database", "mbta")\
        .option("collection", "vehicles").save()

# Write data to MongoDB using foreachBatch
mss = finalStage.writeStream.outputMode("update").foreachBatch(foreach_batch_function).start()

# Group data by "current_status" and "window" and write to memory for stats
csss = finalStage.groupBy('current_status', window(col("timestamp"), "30 seconds", "15 seconds"))\
  .count()\
  .withColumnRenamed('current_status', '_id')\
  .writeStream\
  .queryName('mbta_msgs_stats')\
  .outputMode("complete")\
  .format("memory")\
  .start()

# Define a job to print statistics
tl = Timeloop()

@tl.job(interval=timedelta(seconds=15))
def print_stats():
    df = spark.sql("""select * from mbta_msgs_stats 
                      where window = (select last(window) from 
                            (select * from mbta_msgs_stats where window.end < CURRENT_TIMESTAMP order by window))""")
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database", "mbta")\
        .option("collection", "event_stats").save()

# Start the timeloop job
tl.start(block=True)

# Wait for the streams to terminate
mss.awaitTermination()
