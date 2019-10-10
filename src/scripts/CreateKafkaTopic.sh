#!/usr/bin/bash
/kafka/bin/kafka-topics.sh --create --zookeeper “localhost:2181”  --topic my_kafka_topic --partitions 1 --replication-factor 1
