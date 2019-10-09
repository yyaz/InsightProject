#!/usr/bin/python
spark-submit --master spark://<EC2-instance-address>:7077 --jars /usr/local/spark/jars/spark-streaming_2.11-2.4.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 SparkStructuredStreaming.py

# For Checking Spark Jobs Progress:
# http://${EC-2 instance address}:8080
