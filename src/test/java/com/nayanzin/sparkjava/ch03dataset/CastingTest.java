package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

/* Inspired by https://sparkbyexamples.com/spark/spark-change-dataframe-column-type/ */
public class CastingTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", StringType, false)));

    private static final StructType SCHEMA2 = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", IntegerType, false)));

    private Dataset<Row> df;

    @Before
    public void setUp() {
        df = spark().createDataFrame(asList(
                create("name1", "1"),
                create("name2", "2"),
                create("name3", "3"),
                create("name4", "4")), SCHEMA);
    }

    @Test
    public void castingStringToIntTest() {
        df = spark().createDataFrame(asList(
                create("name1", "1"),
                create("name2", "2")), SCHEMA);
        Dataset<Row> dfWithNumberInt = df
                .withColumn("number_int", col("number").cast(IntegerType))
                .drop("number");
        assertThat(dfWithNumberInt.collectAsList()).containsExactlyInAnyOrder(
                create("name1", 1),
                create("name2", 2));
    }
}