package com.nayanzin.highperformancespark.timeseriesaggregation;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.nayanzin.highperformancespark.timeseriesaggregation.dto.PersonTimeScore;
import com.nayanzin.highperformancespark.utils.SerializableComparator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.Utils;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class RddRepartitionAndSortWithinPartitionTimeSeriesAggregation extends SharedJavaSparkContext implements Serializable {

    public static final int NUMBER_OF_PARTITIONS = 100;

    @Test
    public void test() {
        JavaRDD<PersonTimeScore> data = jsc().parallelize(asList(
                new PersonTimeScore("User1", 3L, 1),
                new PersonTimeScore("User1", 1L, 1),
                new PersonTimeScore("User1", 2L, 0), // exp 1

                new PersonTimeScore("User2", 11L, 1),
                new PersonTimeScore("User2", 23L, 1),
                new PersonTimeScore("User2", 15L, 1), // exp 3

                new PersonTimeScore("User3", 24L, 0),
                new PersonTimeScore("User3", 22L, 1),
                new PersonTimeScore("User3", 10L, 1) // exp 0
        )).repartition(100);

        Partitioner namePartitioner = new Partitioner() {
            @Override
            public int numPartitions() {
                return NUMBER_OF_PARTITIONS;
            }

            @Override
            public int getPartition(Object key) {
                String name = ((PersonTimeScore) key).getName();
                return Utils.nonNegativeMod(name.hashCode(), NUMBER_OF_PARTITIONS);
            }
        };

        List<Tuple2<String, Integer>> usersFinalScores = data
                .mapToPair(row -> new Tuple2<>(row, row.getScore()))
                .repartitionAndSortWithinPartitions(namePartitioner,
                        (SerializableComparator<PersonTimeScore>) (o1, o2) -> Long.compare(o1.getHour(), o2.getHour()))
                .mapPartitions(tuple2Iterator -> {
                    Map<String, Integer> userFinalScoreStorage = new HashMap<>();
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<PersonTimeScore, Integer> tuple2 = tuple2Iterator.next();
                        String name = tuple2._1.getName();
                        int score = tuple2._2;
                        userFinalScoreStorage.compute(name, (k, v) -> {
                            if (score == 0) {
                                return 0;
                            }
                            return isNull(v) ? score : v + score;
                        });
                    }
                    return userFinalScoreStorage.entrySet()
                            .stream()
                            .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                            .collect(toList())
                            .iterator();
                }, true)
                .collect();

        assertThat(usersFinalScores).containsExactlyInAnyOrder(
                new Tuple2<>("User1", 1),
                new Tuple2<>("User2", 3),
                new Tuple2<>("User3", 0));
    }
}