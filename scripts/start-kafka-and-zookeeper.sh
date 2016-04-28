#! /bin/bash
# Kevin Kim, April 2016

KAFKA_DIR="C:/kafka_2.10-0.9.0.1/bin/windows"
KAFKA_CONFIG_DIR="C:/kafka_2.10-0.9.0.1/config"
TOPIC="SpatialData3"
#STOP zookeeper
$KAFKA_DIR/zookeeper-server-stop.bat $KAFKA_CONFIG_DIR/zookeeper.properties

#STOP kafka
$KAFKA_DIR/zookeeper-server-stop.bat $KAFKA_CONFIG_DIR/zookeeper.properties

sleep 5
#Start zookeeper server 
start call $KAFKA_DIR/zookeeper-server-start.bat $KAFKA_CONFIG_DIR/zookeeper.properties

sleep 5
#Start Kafka server in another terminal 
start call $KAFKA_DIR/kafka-server-start.bat $KAFKA_CONFIG_DIR/server.properties

#Create Topic
$KAFKA_Dir/kafka-topics.bat --create --zookeeper localhost:2181--replication-factor 1 --partitions 1 --topic $TOPIC

start $KAFKA_DIR/kafka-console-producer.bat --broker-list localhost:9092 --topic $TOPIC

start $KAFKA_DIR/kafka-console-consumer.bat --zookeeper localhost:2181 --topic $TOPIC


