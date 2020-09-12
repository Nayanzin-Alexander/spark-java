package com.nayanzin.usecases;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CountNonNullColumns extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("A", StringType, false),
            createStructField("B", StringType, false),
            createStructField("C", StringType, false)));

    private Dataset<Row> df;

    @Before
    public void setUp() {
        df = spark().createDataFrame(asList(
                create("name1", "A", null, null),
                create("name1", null, "B", null),
                create("name1", null, "B", "C"),
                create("name1", "A", null, "C"),
                create("name2", null, "B", "C"),
                create("name3", null, null, null)), SCHEMA);
    }

    @Test
    public void countNonNullColumns() {

        // Get column names except the pivot one
        List<String> columns = Arrays.stream(df.schema().fields())
                .map(StructField::name)
                .filter(name -> !name.equals("name"))
                .collect(toList());

        //
        for (String name : columns) {
            df = df.withColumn(name, when(col(name).isNotNull(), 1).otherwise(0));
        }
        df.show();


        // Non null fields by col by name sum
        Column[] cols = new Column[columns.size() - 1];
        int i = -1;
        Column first = null;
        for (String name : columns) {
            if (i == -1) {
                i++;
                first = sum(name).as(name);
            } else {
                cols[i++] = sum(name).as(name);
            }
        }

        df = df.groupBy("name")
                .agg(first, cols);
        for (String name : columns) {
            df = df.withColumn(name, when(col(name).gt(0), 1).otherwise(0));
        }
        df.show();


        // All cols sum
        Column sumOfAllColumnsExpression = col(columns.get(0));
        for (int j = 1; j < columns.size(); j++) {
            sumOfAllColumnsExpression = sumOfAllColumnsExpression.plus(col(columns.get(j)));
        }
        df = df.select(col("name"), sumOfAllColumnsExpression.as("numberOfNotNullColumns"));
        df.show();

        List<Row> result = df.collectAsList();
        assertThat(result).containsExactlyInAnyOrder(
                create("name1", 3),
                create("name2", 2),
                create("name3", 0)
        );
    }
}
