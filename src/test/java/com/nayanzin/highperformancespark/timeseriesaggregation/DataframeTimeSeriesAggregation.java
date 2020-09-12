package com.nayanzin.highperformancespark.timeseriesaggregation;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DataframeTimeSeriesAggregation extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("user", StringType, false),
            createStructField("hour", IntegerType, false),
            createStructField("score", IntegerType, false)));

    @Test
    public void dataframeTest() {
        Dataset<Row> df = spark().createDataFrame(asList(
                create("User1", 22, 1),
                create("User1", 10, 1),
                create("User1", 15, 0), // exp 1

                create("User2", 11, 1),
                create("User2", 23, 1),
                create("User2", 15, 1), // exp 3

                create("User3", 24, 0),
                create("User3", 22, 1),
                create("User3", 10, 1)) // exp 0
                , SCHEMA).repartition(1000);

        List<Tuple2<String, Integer>> usersFinalScores = df.repartition(10, col("user"))
                .sortWithinPartitions("hour")
                .toJavaRDD()
                .mapPartitionsToPair((rowIterator -> {
                    Map<String, Integer> userScores = new HashMap<>();
                    while (rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        String name = row.getString(0);
                        int score = row.getInt(2);
                        userScores.compute(name,
                                (key, value) -> score != 0
                                        ? isNull(value) ? score : value + score
                                        : 0);
                    }
                    return userScores.entrySet()
                            .stream()
                            .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                            .collect(toList())
                            .iterator();
                }))
                .collect();
        assertThat(usersFinalScores).containsExactlyInAnyOrder(
                new Tuple2<>("User1", 1),
                new Tuple2<>("User2", 3),
                new Tuple2<>("User3", 0));
    }
}