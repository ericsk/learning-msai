# Databricks notebook source
# MAGIC %md
# MAGIC #Streaming Mobile Game Events
# MAGIC 
# MAGIC This demo will simulate an end-to-end ETL pipeline for mobile game events
# MAGIC 
# MAGIC #####Steps
# MAGIC - Event Generator that sends events to Azure Event Hubs
# MAGIC - Perform necessary transformations to extract real-time KPI's
# MAGIC - Visualize KPI's on a dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup Instructions
# MAGIC - Start Mobile Event Generator

# COMMAND ----------

# MAGIC %md
# MAGIC #####Setup Azure Event Hubs receiver

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

connectionString = "Endpoint=sb://NAMESPACE.servicebus.windows.net/HUBNAME;EntityPath=HUBNAME;SharedAccessKeyName=POLICY_NAME;SharedAccessKey=POLICY_KEY"
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

eventhubDataFrame = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Defining Incoming Data Schemas

# COMMAND ----------

eventhubSchema = StructType() \
            .add('body', BinaryType()) \
            .add('partition', StringType()) \
            .add('offset', StringType()) \
            .add('sequenceNumber', LongType()) \
            .add('enqueuedTime', TimestampType()) \
            .add('publisher', StringType()) \
            .add('partitionKey', StringType()) \
            .add('properties', StructType() \
                  .add('key', StringType()) \
                  .add('value', StringType()))

eventSchema = StructType().add('eventName', StringType()) \
              .add('eventTime', TimestampType()) \
              .add('eventParams', StructType() \
                   .add('game_keyword', StringType()) \
                   .add('app_name', StringType()) \
                   .add('scoreAdjustment', IntegerType()) \
                   .add('platform', StringType()) \
                   .add('app_version', StringType()) \
                   .add('device_id', StringType()) \
                   .add('client_event_time', TimestampType()) \
                   .add('amount', DoubleType())
                  )            

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read incoming stream

# COMMAND ----------

gamingEventDF = eventhubDataFrame.selectExpr("cast (body as STRING) jsonData") \
.select(from_json('jsonData', eventSchema).alias('body'))\
.select('body.eventName', 'body.eventTime', 'body.eventParams')

# COMMAND ----------

eventsStream = gamingEventDF.filter(gamingEventDF.eventTime.isNotNull()).withColumn("eventDate", to_date(gamingEventDF.eventTime)) \
  .writeStream \
  .format('memory') \
  .queryName('incoming_events') \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Analytics, KPI's, Oh My!

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Events in the last hour?

# COMMAND ----------

countsDF = gamingEventDF.withWatermark("eventTime", "10 minutes").groupBy(window("eventTime", "5 minute")).count()
countsQuery = countsDF.writeStream \
  .format('memory') \
  .queryName('incoming_events_counts') \
  .start()

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bookings in the last 5 minutes?

# COMMAND ----------

bookingsDF = gamingEventDF.withWatermark("eventTime", "10 minutes").filter(gamingEventDF['eventName'] == 'purchaseEvent').groupBy(window("eventTime", "5 minute")).sum("eventParams.amount")
bookingsQuery = bookingsDF.writeStream \
  .format('memory') \
  .queryName('incoming_events_bookings') \
  .start()

# COMMAND ----------

display(bookingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #####OK, how about the housekeeping?
# MAGIC 
# MAGIC A common problem with streaming is that you end up with a lot of small files.  If only there were a way to perform compaction...

# COMMAND ----------

# For the sake of example, let's stop the streams to demonstrate compaction and vacuum
eventsStream.stop()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Where can I learn more?
# MAGIC 
# MAGIC * [Databricks](http://www.databricks.com/)
# MAGIC * [Databricks Guide > Structured Streaming](https://docs.databricks.com/spark/latest/structured-streaming/index.html)
