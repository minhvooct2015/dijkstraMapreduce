#!/bin/bash


# Format HDFS namenode
#hdfs namenode -format


# Start Hadoop services
/usr/local/Cellar/hadoop/3.3.2/sbin/start-all.sh

# Create directories in HDFS
hdfs dfs -mkdir /user
hdfs dfs -mkdir /output
hdfs dfs -mkdir /user/chattod

# Copy data to HDFS
hdfs dfs -copyFromLocal /Users/rickdal/Documents/Hadoop-project/DijkstraMapreduce/data /user/chattod/data

/usr/local/Cellar/hadoop/3.3.2/bin/hadoop jar target/DijkstraMapreduce-1.0-SNAPSHOT.jar Dijkstra /user/chattod/data output
