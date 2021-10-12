from pyspark.sql import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *#Key data points from CoWin database at a district level

cassandra_ip = '172.19.0.3'
kafka_broker='172.19.0.5:9093'
topic='district_stats_daily'

def writeToCassandra(writeDF,epochId):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="india_district_daily_statistics", keyspace="covid_slots")\
    .mode("append")\
    .save()

spark = SparkSession.builder \
      .config("spark.cassandra.connection.host",cassandra_ip)\
      .config("spark.driver.allowMultipleContexts", "true")\
      .master('local') \
      .appName('India Daily Statistics') \
      .getOrCreate()

schema = StructType()\
      .add("confirmed_total",StringType())\
      .add("date",StringType())\
      .add("district",StringType())\
      .add("deceased_total",StringType())\
      .add("other_total",StringType())\
      .add("recovered_total",StringType())\
      .add("state",StringType())\
      .add("tested_total",StringType())\

df   = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_broker) \
      .option("subscribe", topic) \
      .option("startingOffsets", "latest") \
      .load()\
      .selectExpr("CAST(value AS STRING)")\
      .select(from_json(col('value'),schema).alias("kar_daily_statistics"))\
      .select(col('kar_daily_statistics.confirmed_total').alias('confirmed_total')
        ,col('kar_daily_statistics.date').alias('date')
        ,col('kar_daily_statistics.district').alias('district')
        ,col('kar_daily_statistics.deceased_total').alias('deceased_total')
        ,col('kar_daily_statistics.other_total').alias('other_total')
        ,col('kar_daily_statistics.recovered_total').alias('recovered_total')
        ,col('kar_daily_statistics.state').alias('state')
        ,col('kar_daily_statistics.tested_total').alias('tested_total'))



print("Stream starting")
df.printSchema()
# writestream = df.writeStream.trigger(processingTime='5 seconds').outputMode('update').option('truncate','false').format('console').start()
# writestream.awaitTermination()

writestream = df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(writeToCassandra) \
                    .start()
print("Awaiting Termination")
writestream.awaitTermination()
print(writestream.lastprogress)