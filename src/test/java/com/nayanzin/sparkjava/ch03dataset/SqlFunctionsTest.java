package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlFunctionsTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("double", DoubleType, true),
            createStructField("decimal", DecimalType.apply(10, 5), true),
            createStructField("array", ArrayType.apply(IntegerType), true),
            createStructField("string", StringType, true)));

    private static final MapFunction<Row, Double> ROW_TO_DOUBLE_MAP_FUNCTION = row -> row.isNullAt(0) ? null : row.getDouble(0);
    private static final MapFunction<Row, BigDecimal> ROW_TO_BIG_DECIMAL_MAP_FUNCTION = row -> row.isNullAt(0) ? null : row.getDecimal(0);
    private static final MapFunction<Row, Boolean> ROW_TO_BOOLEAN_MAP_FUNCTION = row -> row.isNullAt(0) ? null : row.getBoolean(0);
    private static final MapFunction<Row, Integer> ROW_TO_INTEGER_MAP_FUNCTION = row -> row.isNullAt(0) ? null : row.getInt(0);
    private static final MapFunction<Row, String> ROW_TO_STRING_MAP_FUNCTION = row -> row.isNullAt(0) ? null : row.getString(0);

    @Test
    public void sqlFunctionsTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create(1.0D, BigDecimal.valueOf(1.5D), asList(1, 11, 111), null),
                create(null, BigDecimal.valueOf(2.5D), null, "2"),
                create(9.0D, BigDecimal.valueOf(3.5D), asList(9, 99, 999), "3.0"),
                create(16.0D, null, asList(16, 1616, null), "text")), SCHEMA);

        assertThat(collectAsListOfBooleans(df.select(not(isnan(col("string"))))))
                .as("not(isNAN) functions")
                .containsExactly(true, true, true, true);

        assertThat(collectAsListOfDoubles(df.select(sqrt("double"))))
                .as("sqrt function")
                .containsExactly(1.0D, null, 3D, 4D);

        assertThat(collectAsListOfDecimals(df.select(ceil("decimal"))))
                .as("ceil function")
                .containsExactly(BigDecimal.valueOf(2), BigDecimal.valueOf(3), BigDecimal.valueOf(4), null);

        assertThat(collectAsListOfBooleans(df.select(array_contains(col("array"), 99))))
                .as("array_contains function")
                .containsExactly(false, null, true, null);

        assertThat(collectAsListOfIntegers(df.select(explode(col("array")))))
                .as("explode function")
                .containsExactly(1, 11, 111, 9, 99, 999, 16, 1616, null);

        assertThat(collectAsListOfString(df.select(
                when(col("string").contains("ext"), "Contains 'ext'")
                .when(col("string").contains("3"), "Contains '3'")
                .when(col("string").isNull(), "Is null")
                .otherwise("Something else"))))
                .as("when/otherwise functions")
                .containsExactly("Is null", "Something else", "Contains '3'", "Contains 'ext'");

        assertThat(collectAsListOfDoubles(df.select(nanvl(col("string"), col("double")))))
                .as("nanvl the same as non-Nan function")
                .containsExactly(null, 2.0D, 3.0D, null);
    }

    private List<Double> collectAsListOfDoubles(Dataset<Row> datasetWithSingleColumn) {
        return datasetWithSingleColumn.map(ROW_TO_DOUBLE_MAP_FUNCTION, Encoders.DOUBLE()).collectAsList();
    }

    private List<BigDecimal> collectAsListOfDecimals(Dataset<Row> datasetWithSingleColumn) {
        return datasetWithSingleColumn.map(ROW_TO_BIG_DECIMAL_MAP_FUNCTION, Encoders.DECIMAL())
                .collectAsList()
                .stream()
                .map(v -> isNull(v) ? null: v.stripTrailingZeros())
                .collect(Collectors.toList());
    }

    private List<Boolean> collectAsListOfBooleans(Dataset<Row> datasetWithSingleColumn) {
        return datasetWithSingleColumn.map(ROW_TO_BOOLEAN_MAP_FUNCTION, Encoders.BOOLEAN()).collectAsList();
    }

    private List<Integer> collectAsListOfIntegers(Dataset<Row> datasetWithSingleColumn) {
        return datasetWithSingleColumn.map(ROW_TO_INTEGER_MAP_FUNCTION, Encoders.INT()).collectAsList();
    }

    private List<String> collectAsListOfString(Dataset<Row> datasetWithSingleColumn) {
        return datasetWithSingleColumn.map(ROW_TO_STRING_MAP_FUNCTION, Encoders.STRING()).collectAsList();
    }
}
