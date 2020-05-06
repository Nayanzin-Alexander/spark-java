package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import static com.nayanzin.sparkjava.util.TestUtils.getTestResource;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DataFrameTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final Path inputFile = getTestResource("/sparkjava/ch03dataset/input.csv");
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

    @Test
    public void describeFunctionTest() {
        Dataset<Row> dataset = spark().createDataFrame(asList(
                create("Alex A.", 10, "AA", 0.1D),
                create("Boris B.", 20, "BB", 0.2D),
                create("Cindy C.", 30, "CC", 0.3D)
        ), csvSchema);
        List<Row> statRow = dataset
                .describe("salary")
                .coalesce(1)
                .collectAsList();
        assertThat(statRow.get(0).getString(1)).as("Count").isEqualTo("3");
        assertThat(statRow.get(1).getString(1)).as("Mean").isEqualTo("0.20000000000000004");
        assertThat(statRow.get(2).getString(1)).as("Stddev").isEqualTo("0.09999999999999998");
        assertThat(statRow.get(3).getString(1)).as("Min").isEqualTo("0.1");
        assertThat(statRow.get(4).getString(1)).as("Max").isEqualTo("0.3");

    }
}
