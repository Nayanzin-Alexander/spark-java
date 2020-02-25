package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.spark.api.java.JavaPairRDD.toRDD;
import static org.apache.spark.api.java.JavaRDD.fromRDD;
import static org.assertj.core.api.Assertions.assertThat;

public class PairRDDActionsTest extends SharedJavaSparkContext implements Serializable {

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
}
