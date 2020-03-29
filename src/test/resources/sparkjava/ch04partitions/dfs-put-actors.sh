#!/usr/bin/env bash
hdfs dfs -mkdir -p /sparkjava/ch04partitions
hdfs dfs -rm /sparkjava/ch04partitions/actors.csv
hdfs dfs -put -p actors.csv /sparkjava/ch04partitions/actors.csv