package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

/* Inspired by https://sparkbyexamples.com/spark/spark-change-dataframe-column-type/ */
public class CastingTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", StringType, false)));

    private static final StructType TIMESTAMP_SCHEMA = createStructType(singletonList(
            createStructField("timestamp", StringType, false)));

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
                .withColumn("conditional", when(col("name").equalTo("name1"), 1).otherwise(0))
                .drop("number");
        assertThat(dfWithNumberInt.collectAsList()).containsExactlyInAnyOrder(
                create("name1", 1, 1),
                create("name2", 2, 0));

        df.agg(countDistinct(col("name"))).show();
    }

    @Test
    public void testHourColumnExtraction() {
        df = spark().createDataFrame(asList(
                create("2019-10-16T15:00:00.000000000Z"),
                create("2019-10-16T01:00:00.000000000Z"),
                create("2019-10-16T00:00:00.000000000Z")
        ), TIMESTAMP_SCHEMA);
        Dataset<Row> dfWithHour = df.withColumn("hour", col("timestamp").substr(12, 2).cast(IntegerType));
        assertThat(dfWithHour.collectAsList()).containsExactlyInAnyOrder(
                create("2019-10-16T15:00:00.000000000Z", 15),
                create("2019-10-16T01:00:00.000000000Z", 1),
                create("2019-10-16T00:00:00.000000000Z", 0));
    }
}
