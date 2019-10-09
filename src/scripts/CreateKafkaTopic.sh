#!/usr/bin/bash
kafka-topics.sh --create --zookeeper “localhost:2181”  --topic $TOPIC --partitions 1 --replication-factor 1
