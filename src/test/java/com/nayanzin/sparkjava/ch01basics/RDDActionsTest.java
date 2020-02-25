package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class RDDActionsTest extends SharedJavaSparkContext implements Serializable {

    @Test
    // Collects data as the List at the Driver node.
    public void collectTest() {
        JavaPairRDD<String, Integer> dataset = jsc().parallelizePairs(asList(
                new Tuple2<>("Hello", 5),
                new Tuple2<>("world", 5)), 2);

        assertThat(dataset.collect()).containsExactlyInAnyOrder(
                new Tuple2<>("Hello", 5),
                new Tuple2<>("world", 5));
    }

    @Test
    // Returns to the driver the number of elements in the dataset.
    public void countTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5);
        assertThat(dataset.count()).isEqualTo(10L);
    }

    @Test
    // CountByValue returns a map of each uniq value with its count
    public void countByValueTest() {
        JavaRDD<String> words = jsc().parallelize(asList("Hello", "world", "hello", "world", "hello", "world"));
        assertThat(words.countByValue())
                .hasSize(3)
                .contains(
                        entry("world", 3L),
                        entry("Hello", 1L),
                        entry("hello", 2L));
    }

    @Test
    // Takes the list of first n elements from a RDD
    public void takeTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5);
        assertThat(dataset.take(3)).containsExactlyInAnyOrder(1, 2, 3);
    }


    @Test
    // Takes the list of any n elements from a RDD
    public void topTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5);
        assertThat(dataset.top(3)).hasSize(3);
    }

    @Test
    // Takes 2 values and returns one. First value usually called accumulator.
    public void reduceTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 100);
        assertThat(dataset.reduce(Integer::sum)).isEqualTo(55);
        assertThat(dataset.reduce(Integer::min)).isEqualTo(1);
        assertThat(dataset.reduce(Integer::max)).isEqualTo(10);
        assertThat(dataset.reduce(Integer::max)).isEqualTo(10);
        assertThat(dataset.reduce((acc, element) -> acc * element)).isEqualTo(3628800);
    }

    @Test
    // Aggregates the elements of each partition. And then the result of all partitions.
    // Similar to reduce but we can set accumulator initial value.
    public void foldTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 100);
        assertThat(dataset.fold(10, Integer::sum)).isEqualTo(1065);
    }

    @Test
    public void aggregateTest() {
        JavaRDD<Integer> dataset = jsc().parallelize(asList(1, 2, 3, 4, 5, 6), 100);
        int sumOfAllEvenNumbers = dataset.aggregate(
                0,  // initial value for every accumulator
                (accum, val) -> (val & 1) == 0 ? accum + val : accum,   // reduce each partition
                Integer::sum    // reduce reduced partitions
        );
        assertThat(sumOfAllEvenNumbers).isEqualTo(12);
    }
}
