package com.nayanzin.sparkjava.ch04partitions;

import com.nayanzin.highperformancespark.utils.Utils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Collections;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/*
INPUT=/sparkjava/ch04partitions/actors.csv
OUTPUT=/sparkjava/ch04partitions/parquet
hdfs dfs -rm -r $OUTPUT
spark-submit \
    --master        yarn \
    --deploy-mode   cluster \
    --name          GenerateParquet \
    --conf          spark.yarn.maxAppAttempts=1 \
    --class         com.nayanzin.sparkjava.ch04partitions.CsvToParquetPartitioned  \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT \
    $OUTPUT
 */
public class CsvToParquetPartitioned {

     public static final String RIC_PARTITION_UDF = "idPartition";

     public static void main(String[] args) {

          int inputSize = 293;
          int inputOutputRatio = 3;
          int estimatedOutputSize = inputSize / inputOutputRatio;
          int outputFileSize = 3;
          int numberOfOutputFiles = estimatedOutputSize / outputFileSize;
          int numberOfDistinctYears = 11;
          int numberOfIdPartitions = numberOfOutputFiles / numberOfDistinctYears;
          HashPartitioner hashPartitioner = new HashPartitioner(numberOfIdPartitions);
          UDF1<Integer, Integer> hashPartitionerUdf = hashPartitioner::getPartition;

          SparkSession sparkSession = Utils.buildSparkSession(null, Collections.emptyMap());
          sparkSession.udf().register("getIdPartition", hashPartitionerUdf, IntegerType);
          sparkSession
                  .read()
                  .option("header", "true")
                  .csv(args[0])
                  .repartition(1000)  // imitate many input files

                  .withColumn(RIC_PARTITION_UDF, callUDF("getIdPartition", col("id").cast(IntegerType)))
                  .as(Encoders.bean(Actor.class))

                  .repartition(numberOfOutputFiles, col("year"), col(RIC_PARTITION_UDF))
                  .write()
                  .partitionBy("year", RIC_PARTITION_UDF)
                  .parquet(args[1]);
     }

    /*
    Given
        estimate input file size 1GB csv file
        conversion ratio is output/input is 1/3
    Task:
        break on to 1 mb parquet
        10 mb parquet
        100 mb parquet
     */
}
