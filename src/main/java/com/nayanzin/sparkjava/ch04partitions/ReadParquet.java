package com.nayanzin.sparkjava.ch04partitions;

import com.nayanzin.highperformancespark.Utils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

import static com.nayanzin.highperformancespark.Utils.setStageName;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/*
INPUT=100/
spark-submit \
    --master    local[4] \
    --name      ReadParquet \
    --class     com.nayanzin.sparkjava.ch04partitions.ReadParquet  \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT
 */
public class ReadParquet {

    private static HashPartitioner partitioner = new HashPartitioner(10);

    public static void main(String[] args) {
        SparkSession spark = Utils.buildSparkSession(null, Collections.emptyMap());

        setStageName(spark, ".printSchema()");
        spark.read()
                .parquet(args[0])
                .printSchema();


        setStageName(spark, ".count()");
        spark.read()
                .parquet(args[0])
                .count();

        setStageName(spark, ".select(col(\"id\").equalTo(100L");
        spark.read()
                .parquet(args[0])
                .select(col("id").equalTo(100L))
                .count();

        setStageName(spark, ".filter(col(\"id\").equalTo(100L))\n" +
                "                .agg(sum(col(\"id\")");
        spark.read()
                .parquet(args[0])
                .filter(col("id").equalTo(100L))
                .agg(sum(col("id")));

        setStageName(spark, ".filter(col(\"predicate\").equalTo(partitioner.getPartition(100L)))\n" +
                "                .filter(col(\"id\").equalTo(100L))\n" +
                "                .agg(sum(col(\"id\")));");
        spark.read()
                .parquet(args[0])
                .filter(col("partition").equalTo(partitioner.getPartition(100L)))
                .filter(col("id").equalTo(100L))
                .agg(sum(col("id")));

        setStageName(spark, ".select(col(\"name\").contains(\"Text_2\")");
        spark.read()
                .parquet(args[0])
                .select(col("text").contains("Text_2"))
                .count();
    }
}
