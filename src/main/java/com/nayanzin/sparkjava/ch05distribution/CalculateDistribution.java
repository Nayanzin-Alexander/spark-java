package com.nayanzin.sparkjava.ch05distribution;

import com.nayanzin.sparkjava.ch05distribution.dto.DistributionRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static java.util.Objects.nonNull;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class CalculateDistribution {


    public <T extends Number> Dataset<DistributionRow> calculate(SparkSession spark, Dataset<Row> dataset, String keyColName, String valueColName, int rangeWidth, Long totalCount) {
        spark.udf().register("getDiscret", (T val) -> (val.intValue() / rangeWidth) * rangeWidth, IntegerType);
        // Break to ranges
        Dataset<Row> intermediateDF = dataset
                .withColumn("discreetValue", callUDF("getDiscret", col(valueColName)))
                .groupBy(col("discreetValue"))
                .agg(count(valueColName)).as("discritValueCount");

        // Get total count Count if not provided
        long tc = nonNull(totalCount) ? totalCount : intermediateDF
                .agg(sum("discritValueCount"))
                .collectAsList().stream().map(row -> row.getLong(0)).reduce(0L, Long::sum);

        // count probability for each range
        spark.udf().register("coutPobability", (T count) -> count.doubleValue() / tc, DoubleType);
        return intermediateDF
                .withColumn("probability", callUDF("coutPobability", col("discritValueCount")))
                .as(Encoders.bean(DistributionRow.class));
    }
}
