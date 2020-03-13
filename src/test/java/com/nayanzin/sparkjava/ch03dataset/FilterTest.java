package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.types.DataTypes.*;

public class FilterTest extends JavaDatasetSuiteBase implements Serializable {

    private static final StructType dfSchema = createStructType(asList(
            createStructField("int", IntegerType, true),
            createStructField("bool", BooleanType, true),
            createStructField("arrayOfInt", ArrayType.apply(IntegerType, false), true),
            createStructField("string", StringType, true)));

    @Test
    public void lessThanTest() {
        Dataset<Row> ds = spark().createDataFrame(asList(
                create(10, true, asList(1, 2, 3), "some text"),
                create(20, true, asList(4, 5, 6), "other text")), dfSchema);

        Dataset<Row> intLessThan20 = ds.filter(ds.col("int").lt(20));

        assertDataFrameEquals(intLessThan20, spark().createDataFrame(Collections.singletonList(
                create(10, true, asList(1, 2, 3), "some text")), dfSchema));
    }
}
