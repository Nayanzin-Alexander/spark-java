package com.nayanzin.highperformancespark.timeseriesaggregation;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.nayanzin.highperformancespark.timeseriesaggregation.dto.PersonTimeScore;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RddGroupByMapValuesTimeSeriesAggregation extends SharedJavaSparkContext implements Serializable {

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

        List<Tuple2<String, Integer>> usersFinalScores = data
                .mapToPair(line -> new Tuple2<>(line.getName(), new Tuple2<>(line.getHour(), line.getScore())))
                .groupByKey()
                .mapValues(timeScores -> {
                    AtomicInteger counter = new AtomicInteger(0);
                    StreamSupport.stream(timeScores.spliterator(), false)
                            .sorted((tuple1, tuple2) -> tuple1._1.compareTo(tuple2._1))
                            .forEachOrdered(tuple2 -> {
                                if (tuple2._2 == 0) {
                                    counter.set(0);
                                } else {
                                    counter.incrementAndGet();
                                }
                            });
                    return counter.get();
                })
                .collect();
        assertThat(usersFinalScores).containsExactlyInAnyOrder(
                new Tuple2<>("User1", 1),
                new Tuple2<>("User2", 3),
                new Tuple2<>("User3", 0));
    }
}