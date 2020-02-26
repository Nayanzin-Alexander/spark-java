package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class PairRDDActionsTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void countByKeyTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("A", 11),
                new Tuple2<>("A", 111),
                new Tuple2<>("B", 2),
                new Tuple2<>("B", 22),
                new Tuple2<>("C", 3)));
        Map<String, Long> countByKeyMap = dataset.countByKey();
        assertThat(countByKeyMap).hasSize(3);
        assertThat(countByKeyMap).containsExactly(
                entry("A", 3L),
                entry("B", 2L),
                entry("C", 1L));
    }

    @Test
    public void collectAsMapTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("A", 11),
                new Tuple2<>("A", 111),
                new Tuple2<>("B", 2),
                new Tuple2<>("B", 22),
                new Tuple2<>("C", 3)));
        Map<String, Integer> collectedAsMap = dataset.collectAsMap();

        assertThat(collectedAsMap).containsOnly(
                entry("A", 111),
                entry("B", 22),
                entry("C", 3));
    }

    @Test
    public void lookupTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("A", 11),
                new Tuple2<>("A", 111),
                new Tuple2<>("B", 2),
                new Tuple2<>("B", 22),
                new Tuple2<>("C", 3)));
        List<Integer> lookupResult = dataset.lookup("B");
        assertThat(lookupResult).containsExactlyInAnyOrder(2, 22);
    }



}
