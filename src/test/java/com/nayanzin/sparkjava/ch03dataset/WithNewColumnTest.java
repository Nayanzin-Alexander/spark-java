package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class WithNewColumnTest extends JavaDatasetSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("key", StringType, true),
            createStructField("value_1", IntegerType, true),
            createStructField("value_2", IntegerType, true)));

    @Test
    public void minusTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create("key_1", 1, 10),
                create("key_2", null, 20),
                create("key_3", 3, null)), SCHEMA);
        Dataset<Row> withNewColumn = df
                .na().fill(0L, new String[]{"value_1", "value_2"})
                .withColumn("minus", col("value_1").minus(col("value_2")));
        assertThat(withNewColumn.collectAsList()).containsExactlyInAnyOrder(
                create("key_1", 1, 10, -9),
                create("key_2", 0, 20, -20),
                create("key_3", 3, 0, 3));
    }
}
