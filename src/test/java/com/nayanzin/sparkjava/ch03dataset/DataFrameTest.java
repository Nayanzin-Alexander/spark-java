package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Path;

import static com.nayanzin.sparkjava.util.TestUtils.getTestResource;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.types.DataTypes.*;

public class DataFrameTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final Path inputFile = getTestResource("/ch03dataset/input.csv");
    private static final StructType csvSchema = createStructType(asList(
            createStructField("name", StringType, true),
            createStructField("age", IntegerType, true),
            createStructField("state", StringType, true),
            createStructField("salary", DoubleType, true)));

    @Test
    public void readFromCSVTest() {
        Dataset<Row> actualCsv = spark()
                .read()
                .option("header", "true")
                .schema(csvSchema)
                .csv(inputFile.toString());
        Dataset<Row> expectedCsv = spark().createDataFrame(asList(
                create("William Scott", 20, "WA", 445.022D),
                create("John Doe", 34, "CA", 55.2D),
                create("Antony Jones", 35, "CA", 44.22D)), csvSchema);
        assertDataFrameEquals(actualCsv.orderBy(actualCsv.col("name").asc()),
                expectedCsv.orderBy(expectedCsv.col("name").asc()));
    }

    @Test
    public void filterTest() {
        Dataset<Row> dataset = spark().createDataFrame(asList(
                create("Alex A.", 10, "AA", 0.1D),
                create("Boris B.", 20, "BB", 0.2D),
                create("Cindy C.", 30, "CC", 0.3D)
        ), csvSchema);
        Dataset<Row> older15Years = dataset
                .filter(dataset.col("age").gt(15));
        assertDataFrameEquals(older15Years, spark().createDataFrame(asList(
                create("Boris B.", 20, "BB", 0.2D),
                create("Cindy C.", 30, "CC", 0.3D)), csvSchema));
    }
}
