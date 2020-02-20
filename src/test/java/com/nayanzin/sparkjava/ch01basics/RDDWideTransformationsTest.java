package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

// Wide transformations Require data shuffling between nodes.
public class RDDWideTransformationsTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void intersectionTest() {
        JavaRDD<Integer> set1 = jsc().parallelize(asList(1, 2, 3, 4, 5, 5, 5), 2);
        JavaRDD<Integer> set2 = jsc().parallelize(asList(4, 4, 4, 4, 5, 6, 7), 5);
        JavaRDD<Integer> expectedIntersection = jsc().parallelize(asList(4, 5));
        JavaRDD<Integer> actualIntersection = set1.intersection(set2);
        JavaRDDComparisons.assertRDDEquals(actualIntersection, expectedIntersection);
        assertThat(actualIntersection.partitions()).hasSize(5);
    }

    @Test
    public void distinctTest() {
        JavaRDD<Integer> set1 = jsc().parallelize(asList(1, 2, 3, 3, 4, 5, 5, 5, 5, 5, 5), 5);
        JavaRDD<Integer> expectedDistinctOutput = jsc().parallelize(asList(5, 4, 3, 2, 1));
        JavaRDD<Integer> actualDistinctOutput = set1.distinct();
        JavaRDDComparisons.assertRDDEquals(actualDistinctOutput, expectedDistinctOutput);
    }

    @Test
    public void coalesceTest() {
        // Coalesce can only decrease number of partitions
        JavaRDD<Integer> set = jsc().parallelize(asList(1, 2, 3, 3, 4, 5, 5, 5, 5, 5, 5), 5);
        int expectedNumberOfPartitions = 1;
        JavaRDD<Integer> coalescedSet = set.coalesce(expectedNumberOfPartitions);
        assertThat(coalescedSet.getNumPartitions()).isEqualTo(expectedNumberOfPartitions);

        // So increasing number of partitions with coalesce has no effect
        JavaRDD<Integer> tryToIncreaseNumberOfPartitionsSet = set.coalesce(10);
        assertThat(tryToIncreaseNumberOfPartitionsSet.getNumPartitions()).isEqualTo(set.getNumPartitions());
    }

    @Test
    public void repartitionTest() {
        JavaRDD<Integer> set = jsc().parallelize(asList(1, 2, 3, 3, 4, 5, 5, 5, 5, 5, 5), 5);
        int expectedNumberOfPartitions = 100;
        JavaRDD<Integer> repartitionedSet = set.repartition(expectedNumberOfPartitions);
        assertThat(repartitionedSet.getNumPartitions()).isEqualTo(expectedNumberOfPartitions);
    }
}
