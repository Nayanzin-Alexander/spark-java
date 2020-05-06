package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class GroupingFunctionTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("id", LongType, false),
            createStructField("company_id", LongType, false),
            createStructField("salary", DecimalType.apply(10, 5), false)));

    @Test
    public void companyMinMaxAvgSumSalaryTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create(1L, 1L, BigDecimal.valueOf(100L)),
                create(2L, 1L, BigDecimal.valueOf(50L)),
                create(3L, 1L, BigDecimal.valueOf(10L)),
                create(4L, 2L, BigDecimal.valueOf(200L)),
                create(5L, 3L, null)), SCHEMA);

        List<Row> companiesSummary = df
                .filter(col("salary").isNotNull())
                .groupBy(col("company_id"))
                .agg(
                        min("salary").as("min_salary"),
                        max("salary").as("salary_max"),
                        avg("salary").as("salary_avg"),
                        sum("salary").as("salary_sum"))
                .collectAsList();

        assertThat(companiesSummary).containsExactlyInAnyOrder(
                create(1L, new BigDecimal("10.00000"), new BigDecimal("100.00000"), new BigDecimal("53.333333333"), new BigDecimal("160.00000")),
                create(2L, new BigDecimal("200.00000"), new BigDecimal("200.00000"), new BigDecimal("200.00000"), new BigDecimal("200.00000"))
        );
    }

    @Test
    public void nullFieldSum() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create(1L, 1L, null),
                create(2L, 1L, null),
                create(3L, null, null),
                create(4L, null, null),
                create(5L, 3L, null)), SCHEMA);
        long actualSum = df.agg(sum("company_id"))
                .collectAsList().get(0).getLong(0);
        assertThat(actualSum).isEqualTo(5L);
    }
}
