package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.spark.api.java.JavaPairRDD.toRDD;
import static org.apache.spark.api.java.JavaRDD.fromRDD;
import static org.assertj.core.api.Assertions.assertThat;

public class PairRDDTransformationsTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void mapValuesTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 3)));
        JavaPairRDD<String, Integer> actual = dataset.mapValues(value -> (int) Math.pow(value, 3));
        JavaPairRDD<String, Integer> expected = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 8),
                new Tuple2<>("C", 27)));
        ClassTag<Tuple2<String, Integer>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actual), tag),
                fromRDD(toRDD(expected), tag));
    }

    @Test
    public void flatMapValuesTest() {
        JavaPairRDD<String, String> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("Set1", "a b c d e"),
                new Tuple2<>("Set2", "s w x y z")));
        JavaPairRDD<String, String> actual = dataset
                .flatMapValues(value -> Arrays.asList(value.split(" ")));
        JavaPairRDD<String, String> expected = jsc().parallelizePairs(asList(
                new Tuple2<>("Set1", "a"),
                new Tuple2<>("Set1", "b"),
                new Tuple2<>("Set1", "c"),
                new Tuple2<>("Set1", "d"),
                new Tuple2<>("Set1", "e"),
                new Tuple2<>("Set2", "s"),
                new Tuple2<>("Set2", "w"),
                new Tuple2<>("Set2", "x"),
                new Tuple2<>("Set2", "y"),
                new Tuple2<>("Set2", "z")));
        ClassTag<Tuple2<String, String>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actual), tag),
                fromRDD(toRDD(expected), tag));
    }

    @Test
    public void keysTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 3)));
        JavaRDD<String> actualKeys = dataset.keys();
        JavaRDD<String> expectedKeys = jsc().parallelize(asList("A", "B", "C"));
        JavaRDDComparisons.assertRDDEquals(actualKeys, expectedKeys);
    }

    @Test
    public void valuesTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("C", 3)));
        JavaRDD<Integer> actualValues = dataset.values();
        JavaRDD<Integer> expectedKeys = jsc().parallelize(asList(1, 2, 3));
        JavaRDDComparisons.assertRDDEquals(actualValues, expectedKeys);
    }

    @Test
    public void groupByKey() {
        JavaPairRDD<Integer, Long> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>(1, 10L),
                new Tuple2<>(1, 100L),
                new Tuple2<>(2, 55L),
                new Tuple2<>(2, 33L),
                new Tuple2<>(2, 10L)));
        JavaPairRDD<Integer, Iterable<Long>> actualGroupedByKey = dataset.groupByKey();
        Map<Integer, Iterable<Long>> groupedByKeyMap = actualGroupedByKey.collectAsMap();
        assertThat(groupedByKeyMap).hasSize(2);
        assertThat(groupedByKeyMap.get(1)).containsExactlyInAnyOrder(10L, 100L);
        assertThat(groupedByKeyMap.get(2)).containsExactlyInAnyOrder(55L, 33L, 10L);
    }

    @Test
    public void reduceByKeyTest() {
        JavaPairRDD<Integer, Long> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>(1, 5L),
                new Tuple2<>(1, 5L),
                new Tuple2<>(2, 100L),
                new Tuple2<>(2, 100L)));
        JavaPairRDD<Integer, Long> sumByKey = dataset.reduceByKey(Long::sum);
        JavaPairRDD<Integer, Long> expected = jsc().parallelizePairs(asList(
                new Tuple2<>(1, 10L),
                new Tuple2<>(2, 200L)));
        ClassTag<Tuple2<Integer, Long>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(sumByKey), tag),
                fromRDD(toRDD(expected), tag));
    }

    @Test
    public void aggregateByKeyTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("Key1", 1),
                new Tuple2<>("Key1", 2),
                new Tuple2<>("Key1", 3),
                new Tuple2<>("Key1", 4),
                new Tuple2<>("Key1", 5),
                new Tuple2<>("Key2", 1),
                new Tuple2<>("Key2", 3),
                new Tuple2<>("Key2", 5),
                new Tuple2<>("Key2", 7),
                new Tuple2<>("Key2", 9)));
        JavaPairRDD<String, Integer> actualCountOfOddNumbersPerKey = dataset.aggregateByKey(
                0,
                (accum1, val) -> (val & 1) == 1 ? ++accum1 : accum1,
                Integer::sum);
        JavaPairRDD<String, Integer> expectedCountOfOddNumbersPerKey = jsc().parallelizePairs(asList(
                new Tuple2<>("Key1", 3),
                new Tuple2<>("Key2", 5)));
        ClassTag<Tuple2<String, Integer>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualCountOfOddNumbersPerKey), tag),
                fromRDD(toRDD(expectedCountOfOddNumbersPerKey), tag));
    }

    @Test
    public void subtractByKeyTest() {
        JavaPairRDD<Integer, Long> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>(null, 1L),
                new Tuple2<>(1, 10L),
                new Tuple2<>(1, 100L),
                new Tuple2<>(2, 55L),
                new Tuple2<>(3, 33L),
                new Tuple2<>(4, 10L)));
        JavaPairRDD<Integer, String> keysToSubtract = jsc().parallelizePairs(asList(
                new Tuple2<>(null, "c"),
                new Tuple2<>(1, "a"),
                new Tuple2<>(4, "b"),
                new Tuple2<>(5, "c")));
        JavaPairRDD<Integer, Long> actualAfterSubtractByKey = dataset
                .subtractByKey(keysToSubtract);
        JavaPairRDD<Integer, Long> expectedAfterSubtractByKey = jsc().parallelizePairs(asList(
                new Tuple2<>(2, 55L),
                new Tuple2<>(3, 33L)));
        ClassTag<Tuple2<Integer, Long>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualAfterSubtractByKey), tag),
                fromRDD(toRDD(expectedAfterSubtractByKey), tag));
    }

    @Test
    public void coGroupTest() {
        JavaPairRDD<Integer, Long> dataset1 = jsc().parallelizePairs(asList(
                new Tuple2<>(null, 0L),
                new Tuple2<>(1, 1L),
                new Tuple2<>(1, 1L),
                new Tuple2<>(2, 2L)));
        JavaPairRDD<Integer, Long> dataset2 = jsc().parallelizePairs(asList(
                new Tuple2<>(null, 0L),
                new Tuple2<>(1, 11L),
                new Tuple2<>(4, 44L),
                new Tuple2<>(5, 55L)));
        JavaPairRDD<Integer, Tuple2<Iterable<Long>, Iterable<Long>>> actualAfterCoGroup = dataset1.cogroup(dataset2);
        Map<Integer, Tuple2<Iterable<Long>, Iterable<Long>>> actualAfterCoGroupMap = actualAfterCoGroup.collectAsMap();
        assertThat(actualAfterCoGroupMap).hasSize(5);
        assertThat(actualAfterCoGroupMap.get(null)._1).containsExactlyInAnyOrder(0L);
        assertThat(actualAfterCoGroupMap.get(null)._2).containsExactlyInAnyOrder(0L);
        assertThat(actualAfterCoGroupMap.get(1)._1).containsExactlyInAnyOrder(1L, 1L);
        assertThat(actualAfterCoGroupMap.get(1)._2).containsExactlyInAnyOrder(11L);
        assertThat(actualAfterCoGroupMap.get(2)._1).containsExactlyInAnyOrder(2L);
        assertThat(actualAfterCoGroupMap.get(2)._2).isEmpty();
        assertThat(actualAfterCoGroupMap.get(4)._1).isEmpty();
        assertThat(actualAfterCoGroupMap.get(4)._2).containsExactlyInAnyOrder(44L);
        assertThat(actualAfterCoGroupMap.get(5)._1).isEmpty();
        assertThat(actualAfterCoGroupMap.get(5)._2).containsExactlyInAnyOrder(55L);
    }

    @Test
    public void innerJoinTest() {
        JavaPairRDD<Integer, Long> dataset1 = jsc().parallelizePairs(asList(
                new Tuple2<>(0, 0L),
                new Tuple2<>(1, 1L),
                new Tuple2<>(1, 11L),
                new Tuple2<>(2, 2L)));
        JavaPairRDD<Integer, String> dataset2 = jsc().parallelizePairs(asList(
                new Tuple2<>(1, "a"),
                new Tuple2<>(1, "b"),
                new Tuple2<>(2, "c"),
                new Tuple2<>(3, "d"),
                new Tuple2<>(3, "e")));
        List<Tuple2<Integer, Tuple2<Long, String>>> innerJoinedRdd = dataset1.join(dataset2).collect();
        assertThat(innerJoinedRdd).hasSize(5);
        assertThat(innerJoinedRdd).containsExactlyInAnyOrder(
                new Tuple2<>(1, new Tuple2<>(1L, "a")),
                new Tuple2<>(1, new Tuple2<>(1L, "b")),
                new Tuple2<>(1, new Tuple2<>(11L, "a")),
                new Tuple2<>(1, new Tuple2<>(11L, "b")),
                new Tuple2<>(2, new Tuple2<>(2L, "c")));
    }

    @Test
    public void leftOuterJoinTest() {
        JavaPairRDD<Integer, Long> dataset1 = jsc().parallelizePairs(asList(
                new Tuple2<>(0, 0L),
                new Tuple2<>(1, 1L),
                new Tuple2<>(1, 11L),
                new Tuple2<>(2, 2L)));
        JavaPairRDD<Integer, String> dataset2 = jsc().parallelizePairs(asList(
                new Tuple2<>(1, "a"),
                new Tuple2<>(1, "b"),
                new Tuple2<>(3, "d"),
                new Tuple2<>(3, "e")));
        List<Tuple2<Integer, Tuple2<Long, Optional<String>>>> innerJoinedRdd = dataset1.leftOuterJoin(dataset2).collect();
        assertThat(innerJoinedRdd).hasSize(6);
        assertThat(innerJoinedRdd).containsExactlyInAnyOrder(
                new Tuple2<>(0, new Tuple2<>(0L, Optional.empty())),
                new Tuple2<>(1, new Tuple2<>(1L, Optional.of("a"))),
                new Tuple2<>(1, new Tuple2<>(1L, Optional.of("b"))),
                new Tuple2<>(1, new Tuple2<>(11L, Optional.of("a"))),
                new Tuple2<>(1, new Tuple2<>(11L, Optional.of("b"))),
                new Tuple2<>(2, new Tuple2<>(2L, Optional.empty())));
    }
}
