package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

/* Inspired by http://queirozf.com/entries/spark-dataframe-examples-window-functions */
public class WindowFunctionTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("purchase_date", DateType, false),
            createStructField("device_group", StringType, false),
            createStructField("price", DecimalType.apply(6, 2), false)));

    private Dataset<Row> sales;

    @Before
    public void setUp() {
        sales = spark().createDataFrame(asList(
                create(Date.valueOf("2019-01-01"), "notebook", BigDecimal.valueOf(600.00)),
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00)),
                create(Date.valueOf("2019-03-05"), "small phone", BigDecimal.valueOf(100.00)),
                create(Date.valueOf("2019-02-20"), "camera", BigDecimal.valueOf(150.00)),
                create(Date.valueOf("2019-01-20"), "small phone", BigDecimal.valueOf(300.00)),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00)),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00)),
                create(Date.valueOf("2019-04-01"), "small phone", BigDecimal.valueOf(50.00))), SCHEMA);
    }

    @Test
    public void addAveragePriceValuesForDeviceGroupTest() {
        Dataset<Row> withAveragePriceForDeviceGroup = sales
                .withColumn("average_price_in_device_group",
                        mean("price").over(Window.partitionBy(col("device_group"))));

        withAveragePriceForDeviceGroup.show();

        assertThat(withAveragePriceForDeviceGroup.collectAsList()).containsExactlyInAnyOrder(
                create(Date.valueOf("2019-01-01"), "notebook", BigDecimal.valueOf(600.00), BigDecimal.valueOf(900)),
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00), BigDecimal.valueOf(900)),
                create(Date.valueOf("2019-03-05"), "small phone", BigDecimal.valueOf(100.00), BigDecimal.valueOf(150)),
                create(Date.valueOf("2019-02-20"), "camera", BigDecimal.valueOf(150.00), BigDecimal.valueOf(225)),
                create(Date.valueOf("2019-01-20"), "small phone", BigDecimal.valueOf(300.00), BigDecimal.valueOf(150)),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00), BigDecimal.valueOf(700)),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00), BigDecimal.valueOf(225)),
                create(Date.valueOf("2019-04-01"), "small phone", BigDecimal.valueOf(50.00), BigDecimal.valueOf(150)));

        withAveragePriceForDeviceGroup.explain();
    }

    @Test
    public void onlyLargestPriceSalesTest() {
        Dataset<Row> onlyLargestPriceSales = sales
                .withColumn("largest_price_in_device_group",
                        max("price").over(Window.partitionBy(col("device_group"))))
                .filter(col("price").equalTo(col("largest_price_in_device_group")));

        onlyLargestPriceSales.show();

        assertThat(onlyLargestPriceSales.collectAsList()).containsExactlyInAnyOrder(
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00), BigDecimal.valueOf(1200.00)),
                create(Date.valueOf("2019-01-20"), "small phone", BigDecimal.valueOf(300.00), BigDecimal.valueOf(300.00)),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00), BigDecimal.valueOf(700)),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00), BigDecimal.valueOf(300.00)));
    }

    @Test
    public void onlyMostRecentDateInGroupTest() {
        Dataset<Row> onlyMostRecentDateInGroup = sales
                .withColumn("most_resent_date_in_device_group",
                        max("purchase_date").over(Window.partitionBy(col("device_group"))))
                .filter(col("purchase_date").equalTo(col("most_resent_date_in_device_group")));

        onlyMostRecentDateInGroup.show();

        assertThat(onlyMostRecentDateInGroup.collectAsList()).containsExactlyInAnyOrder(
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00), Date.valueOf("2019-05-10")),
                create(Date.valueOf("2019-04-01"), "small phone", BigDecimal.valueOf(50.00), Date.valueOf("2019-04-01")),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00), Date.valueOf("2019-02-15")),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00), Date.valueOf("2019-07-01")));
    }

    @Test
    public void cumulativeSumByDateTest() {
        Dataset<Row> withCumulativeSumByDate = sales
                .withColumn("cumulative_sum_by_date",
                        sum("price").over(Window.orderBy(col("purchase_date"))));

        withCumulativeSumByDate.orderBy(col("purchase_date")).show();

        assertThat(withCumulativeSumByDate.collectAsList()).containsExactlyInAnyOrder(
                create(Date.valueOf("2019-01-01"), "notebook", BigDecimal.valueOf(600.00), BigDecimal.valueOf(600)),
                create(Date.valueOf("2019-01-20"), "small phone", BigDecimal.valueOf(300.00), BigDecimal.valueOf(900)),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00), BigDecimal.valueOf(1600)),
                create(Date.valueOf("2019-02-20"), "camera", BigDecimal.valueOf(150.00), BigDecimal.valueOf(1750)),
                create(Date.valueOf("2019-03-05"), "small phone", BigDecimal.valueOf(100.00), BigDecimal.valueOf(1850)),
                create(Date.valueOf("2019-04-01"), "small phone", BigDecimal.valueOf(50.00), BigDecimal.valueOf(1900)),
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00), BigDecimal.valueOf(3100)),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00), BigDecimal.valueOf(3400)));
    }

    @Test
    public void withRowNumberTest() {
        Dataset<Row> withRowNumber = sales
                .withColumn("row_number",
                       row_number().over(Window.orderBy(col("purchase_date"))));

        withRowNumber.orderBy(col("row_number")).show();

        assertThat(withRowNumber.collectAsList()).containsExactlyInAnyOrder(
                create(Date.valueOf("2019-01-01"), "notebook", BigDecimal.valueOf(600.00), 1),
                create(Date.valueOf("2019-01-20"), "small phone", BigDecimal.valueOf(300.00), 2),
                create(Date.valueOf("2019-02-15"), "large phone", BigDecimal.valueOf(700.00), 3),
                create(Date.valueOf("2019-02-20"), "camera", BigDecimal.valueOf(150.00), 4),
                create(Date.valueOf("2019-03-05"), "small phone", BigDecimal.valueOf(100.00), 5),
                create(Date.valueOf("2019-04-01"), "small phone", BigDecimal.valueOf(50.00), 6),
                create(Date.valueOf("2019-05-10"), "notebook", BigDecimal.valueOf(1200.00), 7),
                create(Date.valueOf("2019-07-01"), "camera", BigDecimal.valueOf(300.00), 8));
    }
}
