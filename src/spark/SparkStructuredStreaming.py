#!/usr/bin/python
'''
This script does the following:
1- Read streaming data from Kafka topic
2- Read static data from S3
3- Use Spark Structured Streaming for processing the data
4- Write to PostgreSQL Database using a psycopg2 connection
'''

from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
import psycopg2

spark = SparkSession.builder.appName('Chain-Track').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# If data source is HDFS
#streamPath = "hdfs://<EC2-instance-address>:9000/event_small/"
#staticPath = "hdfs://<EC2-instance-address>:9000/data_small/"

# If the data source is S3
streamPath = "s3a://<Bucket-name>/event/"
staticPath = "s3a://<Bucket-name>/data/"
outputPath = "s3a://<Bucket-name>/output/"

# Checkpoint location for Kafka
checkpointPath = "hdfs://<EC2-instance-address>:9000/checkpoint-spark/"

def loaddata_and_process():
    '''
    Define schemas for input files and find the closest warehouse for each truck
    Apply the function defined in process_row() for each row of the database
    '''

    # Define strict schemas for input and output datasets for production scenarios and speed up processing
    loadSchema = StructType().add("TEMPERATURE", "string").add("TIMESTAMP", "string").add("TRUCK_ID", "string") \
                        .add("LON", "string") .add("HUMIDITY", "string").add("LOAD_TYPE", "string").add("LAT", "string").add("CUSTOMER_ID", "string")

    whSchema = StructType().add("WAREHOUSE_ID", "string").add("WAREHOUSE_TYPE", "string").add("WH_LON", "double") \
                         .add("WH_LAT", "double")

    # Read the supply chain load data as a stream from Kafka, warehouse data as a static file from S3
    df_wh = spark.read.schema(whSchema).option("sep", ",").option("header", "true").csv(staticPath)

    topic = "my-kafka-topic"
    broker = "<EC2-instance-address>:9092"

    # Subscribe the stream from Kafka
    df_kafka = spark.readStream \
             .format("kafka") \
             .option("kafka.bootstrap.servers", broker) \
             .option("subscribe", topic) \
             .load()

    df_kafka.printSchema()

    #Parse the raw json file and create a dataframe
    df_load = df_kafka.selectExpr("CAST(value AS string)").select(from_json("value", loadSchema).alias("data")).select("data.*") \
                      .selectExpr("CAST(CUSTOMER_ID AS string)" \
                                 ,"CAST(TRUCK_ID AS string)" \
                                 ,"CAST(TEMPERATURE AS double)" \
                                 ,"CAST(HUMIDITY AS double)" \
                                 ,"CAST(LOAD_TYPE AS string)" \
                                 ,"CAST(LON AS double)" \
                                 ,"CAST(LAT AS double)" \
                                 ,"CAST(TIMESTAMP AS timestamp)")

    # To check whether data is correctly taken from Kafka
    #df_load.writeStream \
    #       .format("console") \
    #       .start().awaitTermination()

    print(df_load)

    df_load.createOrReplaceTempView("load")
    df_wh.createOrReplaceTempView("warehouse")

    # Check if data sources are streaming or not
    print("Truck load data is streaming:", df_load.isStreaming)
    print("Warehouse data is streaming:", df_wh.isStreaming)

    df_load.printSchema()
    df_wh.printSchema()

    df_warnTrucks = df_load.where("TEMPERATURE > 32 AND LOAD_TYPE = 'FOOD'")
    df_joinedData = df_warnTrucks.crossJoin(df_wh.where("WAREHOUSE_TYPE = 'FOOD'")) \
                             .withColumn("distance", round(dist("LON", "LAT", 'WH_LON' , 'WH_LAT'),2) \
                             .alias("distance"))

    print("df_joinedData", df_joinedData)
    df_result = df_joinedData.select('CUSTOMER_ID', 'TRUCK_ID', 'TIMESTAMP', struct('distance','WAREHOUSE_ID','WH_LON','WH_LAT','TIMESTAMP','LON','LAT').alias('newstruct')) \
                .groupBy('CUSTOMER_ID', 'TRUCK_ID') \
                .agg(min(col('newstruct')).alias('mindist_to_wh')) \
                .select('CUSTOMER_ID', 'TRUCK_ID', col('mindist_to_wh.WAREHOUSE_ID'), col('mindist_to_wh.WH_LON'), \
               col('mindist_to_wh.WH_LAT'), col('mindist_to_wh.LON'), col('mindist_to_wh.LAT'), col('mindist_to_wh.distance'), col('mindist_to_wh.TIMESTAMP')) \
                .orderBy('CUSTOMER_ID', 'TRUCK_ID')

    print("df_result", df_result)
    print('COMPUTATION FINISHED SUCCESSFULLY')

    # Checking the resultin dataframe on the console for debugging
    # df_result.writeStream \
    #         .format("console") \
    #         .outputMode("complete") \
    #        .start().awaitTermination()

    # Write to memory for debugging
    #query = df_result.writeStream.format("memory").queryName("temp_table").outputMode("complete").start()

    query= df_result \
          .writeStream \
          .foreach(process_row) \
          .outputMode("complete") \
          .start()
    query.awaitTermination()

#Find the closest warehouse for each truck with faulty load
def dist(lon_x, lat_x, lon_y, lat_y):
    '''
    This method calculates the distance in miles of two points x & y given their latitudes and longitudes
    using Haversine formula
    :param lon_x: A float indicating the longitude of the truck
    :param lat_x: A float indicating the latitude of the truck
    :param lon_y: A float indicating the longitude of the warehouse
    :param lat_y: A float indicating the latitude of the warehouse
    :return float: The distance between the truck and the warehouse in miles
    '''
    return acos(
           sin(toRadians(lat_x)) * sin(toRadians(lat_y)) +
           cos(toRadians(lat_x)) * cos(toRadians(lat_y)) *
           cos(toRadians(lon_x) - toRadians(lon_y))
           ) * lit(6371.0/ 1.6093)


def process_row(row):
    '''
    This function is applied to each row of the dataframe and writes each row to the Postgres database
    :param row: A row of the dataframe
    '''
    host = '<DB-instance>'
    database = '<DB-name>'
    user     = '<User-name>'
    password = '<Password>'

    conn = psycopg2.connect("host=" + host + " port=5432" + " dbname=" + database + " user=" +user+ " password=" + password)
    conn.autocommit = True
    curs = conn.cursor()
    valueStr = "'"+ row.CUSTOMER_ID + "'," +"'" + row.TRUCK_ID + "'," +"'" +row.WAREHOUSE_ID+ "'," + "'"+ str(row.WH_LON)+ "'," \
              +"'"+ str(row.WH_LAT) + "'," + "'" + str(row.LON) + "'," + "'" + str(row.LAT) + "'," + "'"+ str(row.distance) + "'," + "'" + str(row.TIMESTAMP) + "'"

    curs.execute("INSERT INTO table_final VALUES (" + valueStr + ")")

    curs.close()
    conn.close()

if __name__ == "__main__":

    loaddata_and_process()
