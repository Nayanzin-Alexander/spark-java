package com.nayanzin.highperformancespark.ch3dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.nayanzin.highperformancespark.Factory.buildSparkSession;
import static java.util.Collections.emptyMap;

/*
./gradlew clean build

INPUT=src/test/resources/highperformancespark/ch3dataset/data.json
spark-submit    \
    --class com.nayanzin.highperformancespark.ch3dataset.PrintJsonSchema    \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT
 */
public class PrintJsonSchema {

    public static void main(String[] args) {
        checkArgs(args);
        SparkSession spark = buildSparkSession("PrintJsonSchema", emptyMap());
        Dataset<Row> json = spark.read().json(args[0]);
        json.printSchema();
    }

    private static void checkArgs(String[] args) {
        if (args.length != 1) {
            System.exit(-1);
        }
    }
}
