#! /bin/bash
# Kevin Kim, April 2016

KAFKA_DIR="C:/kafka_2.10-0.9.0.1/bin/windows"
KAFKA_CONFIG_DIR="C:/kafka_2.10-0.9.0.1/config"

#STOP zookeeper
$KAFKA_DIR/zookeeper-server-stop.bat $KAFKA_CONFIG_DIR/zookeeper.properties

#STOP kafka
$KAFKA_DIR/zookeeper-server-stop.bat $KAFKA_CONFIG_DIR/zookeeper.properties

#Start zookeeper server 
start call $KAFKA_DIR/zookeeper-server-start.bat $KAFKA_CONFIG_DIR/zookeeper.properties

sleep 5
#Start Kafka server in another terminal 
start call $KAFKA_DIR/kafka-server-start.bat $KAFKA_CONFIG_DIR/server.properties




