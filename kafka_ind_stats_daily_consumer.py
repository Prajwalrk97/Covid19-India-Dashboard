from pyspark.sql import *
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *#Key data points from CoWin database at a district level

cassandra_ip = '172.19.0.3'
kafka_broker='172.19.0.5:9093'
topic='kar_stats_daily'

def writeToCassandra(writeDF,epochId):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="india_daily_statistics", keyspace="covid_slots")\
    .mode("append")\
    .save()

spark = SparkSession.builder \
      .config("spark.cassandra.connection.host",cassandra_ip)\
      .config("spark.driver.allowMultipleContexts", "true")\
      .master('local') \
      .appName('India Daily Statistics') \
      .getOrCreate()

schema = StructType()\
      .add("confirmed",StringType())\
      .add("confirmed_total",StringType())\
      .add("confirmed_week_window",StringType())\
      .add("cfr",StringType())\
      .add("cfr_week_window",StringType())\
      .add("date",StringType())\
      .add("deceased",StringType())\
      .add("deceased_total",StringType())\
      .add("deceased_week_window",StringType())\
      .add("recovered",StringType())\
      .add("recovered_total",StringType())\
      .add("recovered_week_window",StringType())\
      .add("state",StringType())\
      .add("tpr",StringType())\
      .add("tpr_week_window",StringType())\
      .add("vaccinated1",StringType())\
      .add("vaccinated1_total",StringType())\
      .add("vaccinated1_week_window",StringType())\
      .add("vaccinated2",StringType())\
      .add("vaccinated2_total",StringType())\
      .add("vaccinated2_week_window",StringType())\

df   = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_broker) \
      .option("subscribe", topic) \
      .option("startingOffsets", "latest") \
      .load()\
      .selectExpr("CAST(value AS STRING)")\
      .select(from_json(col('value'),schema).alias("kar_daily_statistics"))\
      .select(col('kar_daily_statistics.confirmed').alias('confirmed')
        ,col('kar_daily_statistics.confirmed_total').alias('confirmed_total')
        ,col('kar_daily_statistics.confirmed_week_window').alias('confirmed_week_window')
        ,col('kar_daily_statistics.cfr').alias('cfr')
        ,col('kar_daily_statistics.cfr_week_window').alias('cfr_week_window')
        ,col('kar_daily_statistics.date').alias('date')
        ,col('kar_daily_statistics.deceased').alias('deceased')
        ,col('kar_daily_statistics.deceased_total').alias('deceased_total')
        ,col('kar_daily_statistics.deceased_week_window').alias('deceased_week_window')
        ,col('kar_daily_statistics.recovered').alias('recovered')
        ,col('kar_daily_statistics.recovered_total').alias('recovered_total')
        ,col('kar_daily_statistics.recovered_week_window').alias('recovered_week_window')
        ,col('kar_daily_statistics.state').alias('state')
        ,col('kar_daily_statistics.tpr').alias('tpr')
        ,col('kar_daily_statistics.tpr_week_window').alias('tpr_week_window')
        ,col('kar_daily_statistics.vaccinated1').alias('vaccinated1')
        ,col('kar_daily_statistics.vaccinated1_total').alias('vaccinated1_total')
        ,col('kar_daily_statistics.vaccinated1_week_window').alias('vaccinated1_week_window')
        ,col('kar_daily_statistics.vaccinated2').alias('vaccinated2')
        ,col('kar_daily_statistics.vaccinated2_total').alias('vaccinated2_total')
        ,col('kar_daily_statistics.vaccinated2_week_window').alias('vaccinated2_week_window'))


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