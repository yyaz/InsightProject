#!/usr/bin/bash
/kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
/kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties
