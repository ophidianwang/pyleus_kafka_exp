#!/bin/bash
# delete topic
/realtime/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --delete --zookeeper 172.17.24.217:2181,172.17.218.23:2181,172.17.24.219:2181 --topic cep_storm
sleep 3
# create topic 
/realtime/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --create --zookeeper 172.17.24.217:2181,172.17.218.23:2181,172.17.24.219:2181 --partitions 3 --replication-factor 2 --topic cep_storm
sleep 3
# check topic
/realtime/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --describe cep_storm --zookeeper 172.17.24.217:2181,172.17.218.23:2181,172.17.24.219:2181
