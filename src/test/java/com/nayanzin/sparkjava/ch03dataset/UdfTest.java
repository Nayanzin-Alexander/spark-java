package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

/* Inspired by https://medium.com/@contactsunny/apache-spark-sql-user-defined-function-udf-poc-in-java-4af0f38e4757 */
public class UdfTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", StringType, false)));

    @Test
    public void pow2UdfTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create("name1", "1"),
                create("name2", "2"),
                create("name3", "3"),
                create("name4", "4")), SCHEMA);

        UDF1<String, Integer> udf = input -> {
            try {
                return (int) Math.pow(Integer.parseInt(input), 2);
            } catch (NumberFormatException nfe) {
                return null;
            }
        };
        spark().udf().register("udf", udf, IntegerType);
        Dataset<Row> dfWithNumberInt = df
                .withColumn("number_pow_2", callUDF("udf", col("number")));

        assertThat(dfWithNumberInt.collectAsList()).containsExactlyInAnyOrder(
                create("name1", "1", 1),
                create("name2", "2", 4),
                create("name3", "3", 9),
                create("name4", "4", 16));
    }

    @Test
    public void concatColumnsUdfTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create("name1", "1"),
                create("name2", "2"),
                create("name3", "3"),
                create("name4", "4")), SCHEMA);

        UDF2<String, String, String> udf = (col1, col2) -> col1 + " = " + col2;
        spark().udf().register("udf", udf, StringType);
        Dataset<Row> dfWithNumberInt = df
                .withColumn("concat", callUDF("udf", col("name"), col("number")));

        assertThat(dfWithNumberInt.collectAsList()).containsExactlyInAnyOrder(
                create("name1", "1", "name1 = 1"),
                create("name2", "2", "name2 = 2"),
                create("name3", "3", "name3 = 3"),
                create("name4", "4", "name4 = 4"));
    }
}
