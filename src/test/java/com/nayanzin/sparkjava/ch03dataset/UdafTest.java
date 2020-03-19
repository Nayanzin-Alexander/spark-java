package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Option;

import java.io.Serializable;
import java.util.Collections;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

/* Inspired by https://ankithoodablog.wordpress.com/2017/09/07/working-with-spark-udaf-in-java/ */
public class UdafTest extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", StringType, false)));

    @Test
    public void udafTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create("name1", "bad number1"),
                create("name1", "bad number 2"),
                create("name1", "1"),
                create("name2", "2"),
                create("name3", "3"),
                create("name4", "4")), SCHEMA)
                .repartition(200);

        spark().udf().register("udf", new StatsUdaf());
        Dataset<Row> dfWithNumberInt = df
                .agg(callUDF("udf", col("number")).as("stat_array"));


        dfWithNumberInt.show(1, 60);

        Row structRow = dfWithNumberInt.collectAsList().get(0);
        Row statsRow = structRow.getStruct(0);

        assertThat(statsRow.getInt(0)).isEqualTo(1);    // min, IntegerType, nullable
        assertThat(statsRow.getInt(1)).isEqualTo(4);    // max, IntegerType, nullable
        assertThat(statsRow.getLong(2)).isEqualTo(10L);    // sum, LongType, non_nul
        assertThat(statsRow.getLong(3)).isEqualTo(4L);    // count, LongType, non_nul
        assertThat(statsRow.getLong(4)).isEqualTo(2L);    // not_valid_number_count, LongType, non_nul
        assertThat(statsRow.getDouble(5)).isEqualTo(2.5D);    // avg, DoubleType, non_nul
    }

    public static class StatsUdaf extends UserDefinedAggregateFunction {

        @Override
        public StructType inputSchema() {
            return createStructType(Collections.singletonList(createStructField("number", StringType, false)));
        }

        @Override
        public StructType bufferSchema() {
            return createStructType(asList(
                    createStructField("min", IntegerType, true),
                    createStructField("max", IntegerType, true),
                    createStructField("sum", LongType, false),
                    createStructField("count", LongType, false),
                    createStructField("not_valid_number_count", LongType, false)));
        }

        @Override
        public DataType dataType() {
            return createStructType(asList(
                    createStructField("min", IntegerType, true),
                    createStructField("max", IntegerType, true),
                    createStructField("sum", LongType, false),
                    createStructField("count", LongType, false),
                    createStructField("not_valid_number_count", LongType, false),
                    createStructField("avg", DoubleType, true)));
        }

        /* If the UDAF logic is such that the result is independent of the order in which data is
        processed and combined then the UDAF is deterministic. */
        @Override
        public boolean deterministic() {
            return true;
        }

        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, null);     // min, IntegerType, nullable
            buffer.update(1, null);     // max, IntegerType, nullable
            buffer.update(2, 0L);       // sum, LongType, nonnull
            buffer.update(3, 0L);       // count, LongType, nonnull
            buffer.update(4, 0L);       // not_valid_number_count, LongType, nonnull
        }

        @Override
        public void update(MutableAggregationBuffer buff, Row input) {
            int val;
            try {
                val = Integer.parseInt(input.getString(0));
            } catch (NumberFormatException nfe) {
                buff.update(4, buff.getLong(4) + 1); // not_valid_number_count, LongType, nonnull
                return;
            }
            int min = buff.isNullAt(0)
                    ? val
                    : min(buff.getInt(0), val);
            int max = buff.isNullAt(1)
                    ? val
                    : max(buff.getInt(1), val);
            buff.update(0, min); // min, IntegerType, nullable
            buff.update(1, max); // max, IntegerType, nullable
            buff.update(2, buff.getLong(2) + val); // sum, LongType, nonnull
            buff.update(3, buff.getLong(3) + 1); // count, LongType, nonnull
        }

        @Override
        public void merge(MutableAggregationBuffer buff1, Row buff2) {
            // Determine min
            Integer min1 = buff1.isNullAt(0) ? null : buff1.getInt(0);
            Integer min2 = buff2.isNullAt(0) ? null : buff2.getInt(0);
            Integer min;
            if (nonNull(min1) && nonNull(min2)) {
                min = min(min1, min2);
            } else {
                min = nonNull(min1) ? min1 : min2;
            }

            // Determine max
            Integer max1 = buff1.isNullAt(1) ? null : buff1.getInt(1);
            Integer max2 = buff2.isNullAt(1) ? null : buff2.getInt(1);
            Integer max;
            if (nonNull(max1) && nonNull(max2)) {
                max = max(max1, max2);
            } else {
                max = nonNull(max1) ? max1 : max2;
            }

            buff1.update(0, min);
            buff1.update(1, max);
            buff1.update(2, buff1.getLong(2) + buff2.getLong(2));        // sum, LongType, nonnull
            buff1.update(3, buff1.getLong(3) + buff2.getLong(3));        // count, LongType, nonnull
            buff1.update(4, buff1.getLong(4) + buff2.getLong(4));        // not_valid_number_count, LongType, nonnull
        }

        @Override
        public Row evaluate(Row buffer) {
            return create(
                    buffer.isNullAt(0) ? Option.empty() : buffer.getInt(0), // min, IntegerType, nullable),
                    buffer.isNullAt(1) ? Option.empty() : buffer.getInt(1), // max, IntegerType, nullable),
                    buffer.getLong(2),                              // sum, LongType, non_nul),
                    buffer.getLong(3),                              // count, LongType, non_nul),
                    buffer.getLong(4),                              // not_valid_number_count, LongType, non_nul),
                    buffer.getLong(3) > 0
                            ? (double) buffer.getLong(2) / buffer.getLong(3)
                            : 0                                        // avg, DoubleType, non_nul)));
            );
        }
    }
}
