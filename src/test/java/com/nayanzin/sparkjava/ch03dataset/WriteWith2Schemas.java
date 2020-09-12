package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalTime;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

public class WriteWith2Schemas extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType inputSchema = createStructType(asList(
            createStructField("name", StringType, true),
            createStructField("type", IntegerType, true),
            createStructField("a", IntegerType, true),
            createStructField("b", IntegerType, true)));

    private static final StructType outputASchema = createStructType(asList(
            createStructField("name", StringType, true),
            createStructField("type", IntegerType, true),
            createStructField("a", IntegerType, true)));

    private static final StructType outputBSchema = createStructType(asList(
            createStructField("name", StringType, true),
            createStructField("type", IntegerType, true),
            createStructField("b", IntegerType, true)));

    @Test
    public void writeCsvWith2Schemas() {
        LocalTime start = LocalTime.now();
        Dataset<Row> input = spark().createDataFrame(asList(
                create("Alex", 0, 1, null),
                create("Alex", 1, null, 1)
        ), inputSchema);


        input
                .repartition(2, col("type"))
                .write()
                .partitionBy("type")
                .mode(SaveMode.Overwrite)
                .json("/home/oleksandr-nayanzin/IdeaProjects/study/spark-java/type_test");
    }
}
