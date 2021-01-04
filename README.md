# Kafka Streams

## About

This application has samples for different kafka stream operations like :

* Filter/FilterNot
* Map/MapValues
* FlatMap/FlatMapValues
* Peek
* Print
* Foreach
* GroupBy
* GroupByKey
* SelectKey
* Repartition
* Merge
* Branch
* Cogroup
* Aggregate(Windowed/Non Windowed)
* Reduce(Windowed/Non Windowed)
* Count(Windowed/Non Windowed)
* Join

This is developed in Java 8 and Kafka Streams 2.7.0 

Kafka cluster is prerequisite to run these applications. Follow ```Kafka Cluster Setup``` section to set it up.

## Kafka Cluster Setup

#### Follow below steps to set up 3 node cluster on single Mac machine

* Download Kafka from ```https://kafka.apache.org/downloads```
* Extract it somewhere by executing tar command on Terminal

  ```i.e. tar -xvf kafka_2.13-2.6.0.tgz```
* Go to that extracted Kafka folder
   
  ```i.e. cd kafka_2.13-2.6.0/```
* Start zookeeper
   
  ```bin/zookeeper-server-start.sh config/zookeeper.properties```
  
  This will bring up zookeeper on default port 2181 configured in ```config/zookeeper.properties``` file
* Start first broker/node

  ```bin/kafka-server-start.sh config/server.properties```  
  
  This will start broker with below default broker id, log directory and port configured in ```config/server.properties```
  ```   
  broker.id=0  
  log.dirs=/tmp/kafka-logs  
  port=9092
  ``` 
* Create a copy of ```config/server.properties``` file for second broker/node
   
  ```i.e. cp config/server.properties config/server1.properties```
* Change broker id, log directory and port in ```config/server1.properties``` file
   
  ```
  broker.id=1
  log.dirs=/tmp/kafka-logs-1
  port=9093
  ```
* Start second broker/node

  ```bin/kafka-server-start.sh config/server1.properties```    
* Create one more copy of ```config/server.properties``` file for third broker/node

  ```i.e. cp config/server.properties config/server2.properties```
* Change broker id, log directory and port in ```config/server2.properties``` file
   
  ```
  broker.id=2
  log.dirs=/tmp/kafka-logs-2
  port=9094
  ```   
* Start third broker/node

   ```bin/kafka-server-start.sh config/server2.properties```     
   
* Check what brokers are up and running

  ```bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids```  
  
  will give you below output
  
  ```
  Connecting to localhost:2181
 
  WATCHER::
  
  WatchedEvent state:SyncConnected type:None path:null
  [0, 1, 2]
  ``` 