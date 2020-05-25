package com.nayanzin.highperformancespark.ch4joins.tool;

import com.nayanzin.highperformancespark.utils.Utils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Collections;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

/*
Usage:
INPUT=src/test/resources/highperformancespark/ch4joins/addresses_70mb.csv
OUTPUT=src/test/resources/highperformancespark/ch4joins/parquet_addresses
rm -r $OUTPUT
spark-submit \
    --master    local[4] \
    --name      CsvToParquetPartitioned \
    --class     com.nayanzin.highperformancespark.ch4joins.tool.CsvToParquetPartitioned  \
    --conf="spark.driver.memory=5G" \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT \
    $OUTPUT
 */
public class CsvToParquetPartitioned {
    public static void main(String[] args) {
        SparkSession spark = Utils.buildSparkSession(null, Collections.emptyMap());
        UDF1<String, Integer> udf = input -> input.hashCode() % 15;
        spark.udf().register("customPartitioner", udf, IntegerType);
        spark.read()
                .option("header", "true")
                .csv(args[0])
                .withColumn("partition", callUDF("customPartitioner", col("id")))
                .write()
                .partitionBy("partition")
                .parquet(args[1]);
    }
}
