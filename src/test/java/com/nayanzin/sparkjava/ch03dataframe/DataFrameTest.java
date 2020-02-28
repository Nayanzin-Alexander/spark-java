package com.nayanzin.sparkjava.ch03dataframe;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import com.holdenkarau.spark.testing.SparkSessionProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Path;

import static com.nayanzin.sparkjava.util.TestUtils.getTestResource;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.types.Metadata.empty;

public class DataFrameTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final Path inputFile = getTestResource("/ch03dataframe/input.csv");
    private static final StructType csvSchema = new StructType(new StructField[]{
            new StructField("name", DataTypes.StringType, true, empty()),
            new StructField("age", DataTypes.IntegerType, true, empty()),
            new StructField("state", DataTypes.StringType, true, empty()),
            new StructField("salary", DataTypes.DoubleType, true, empty())});

    @Test
    public void a() {
        SparkSession spark = SparkSessionProvider.sparkSession();


        Dataset<Row> df = spark
                .read()
                .option("header", "true")
                .schema(csvSchema)
                .csv(inputFile.toString());

        df.printSchema();

        Dataset<Row> actualOlderThan30 = df.filter(df.col("age").gt(30));


        Dataset<Row> expectedOlderThan30 = spark.createDataFrame(asList(
                RowFactory.create("John Doe", 34, "CA", 55.2D),
                RowFactory.create("Antony Jones", 35, "CA", 44.22)
        ), csvSchema);

        Row[] ar = actualOlderThan30.collect();
        Row[] er = expectedOlderThan30.collect();

        assertDataFrameEquals(actualOlderThan30, expectedOlderThan30);
        // todo https://courses.epam.com/courses/course-v1:EPAM+101BD+0819/courseware/a423c18d5dfb4a5fa5ac8fa819b9e29d/1f97bd4659cb4e958af4611b19fa14bc/1

        // csv.printSchema();
    }
}
