package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.spark.api.java.JavaPairRDD.toRDD;
import static org.apache.spark.api.java.JavaRDD.fromRDD;

public class RDDNarrowTransformationsTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void mapTest() {
        // Given input RDD
        JavaRDD<String> input = jsc().parallelize(asList("Hello", "World"));

        // Create expected output pairRDD
        JavaPairRDD<String, Integer> expectedOutput = jsc().parallelizePairs(asList(
                new Tuple2<>("Hello", 5),
                new Tuple2<>("World", 5)));

        // When map RDD to PairRDD
        JavaPairRDD<String, Integer> actualOutput = input
                .mapToPair(word -> new Tuple2<>(word, word.length()));

        // Then assert map output is equals to expected pairRDD.
        ClassTag<Tuple2<String, Integer>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualOutput), tag),
                fromRDD(toRDD(expectedOutput), tag));
    }

    @Test
    public void flatMapTest() {
        JavaRDD<String> lines = jsc().parallelize(asList("Hello world!!!", "I am Spark Developer!!!"));
        JavaRDD<String> expectedWords = jsc().parallelize(asList("Hello", "world!!!", "I", "am", "Spark", "Developer!!!"));
        JavaRDD<String> actualWorlds = lines.flatMap(line -> asList(line.split(" ")).iterator());
        JavaRDDComparisons.assertRDDEquals(actualWorlds, expectedWords);
    }

    @Test
    public void filterTest() {
        JavaRDD<Integer> input = jsc().parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> expectedOdds = jsc().parallelize(asList(1, 3, 5, 7, 9));
        JavaRDD<Integer> actualOdds = input.filter(number -> (number & 1) == 1);
        JavaRDDComparisons.assertRDDEquals(actualOdds, expectedOdds);
    }

    @Test
    public void unionTest() {
        JavaRDD<Integer> set1 = jsc().parallelize(asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> set2 = jsc().parallelize(asList(5, 6, 7, 8));
        JavaRDD<Integer> expectedSet = jsc().parallelize(
                asList(1, 2, 3, 4, 5, 5, 6, 7, 8));
        JavaRDD<Integer> actualSet = set1.union(set2);
        JavaRDDComparisons.assertRDDEquals(actualSet, expectedSet);
    }

    @Test
    public void mapPartitionTest() {
        JavaRDD<String> input = jsc().parallelize(asList(
                "Hello", "world", "I", "am", "Spark", "developer", "!!!"), 2);
        JavaRDD<Integer> expectedWordsCountInEachPartition = jsc().parallelize(asList(3, 4));
        JavaRDD<Integer> actualWordCountForEachPartition = input.mapPartitions(
                iterator -> {
                    int wordCounter = 0;
                    while (iterator.hasNext()) {
                        wordCounter++;
                        iterator.next();
                    }
                    return singletonList(wordCounter).iterator();
                });
        JavaRDDComparisons.assertRDDEquals(actualWordCountForEachPartition, expectedWordsCountInEachPartition);
    }

    @Test
    public void mapPartitionWithIndexTest() {
        JavaRDD<String> input = jsc().parallelize(asList(
                "Hello", "world", "I", "am", "Spark", "developer", "!!!"), 2);

        JavaRDD<Tuple2<Integer, Integer>> expectedWordsCountInEachPartition = jsc().parallelize(asList(
                new Tuple2<>(3, 0),
                new Tuple2<>(4, 1)));

        JavaRDD<Tuple2<Integer, Integer>> actualWordCountForEachPartition = input.mapPartitionsWithIndex(
                (partitionIndex, iterator) -> {
                    int wordCounter = 0;
                    while (iterator.hasNext()) {
                        wordCounter++;
                        iterator.next();
                    }
                    return singletonList(new Tuple2<>(wordCounter, partitionIndex)).iterator();
                }, true);

        JavaRDDComparisons.assertRDDEquals(actualWordCountForEachPartition, expectedWordsCountInEachPartition);
    }

    @Test
    public void zipTest() {
        JavaRDD<Integer> numbers = jsc().parallelize(asList(1, 2, 3, 4, 5));
        JavaRDD<Character> chars = jsc().parallelize(asList('a', 'b', 'c', 'd', 'e'));
        JavaPairRDD<Integer, Character> expectedZipOutput = jsc().parallelizePairs(asList(
                new Tuple2<>(1, 'a'),
                new Tuple2<>(2, 'b'),
                new Tuple2<>(3, 'c'),
                new Tuple2<>(4, 'd'),
                new Tuple2<>(5, 'e')));
        JavaPairRDD<Integer, Character> actualZipOutput = numbers.zip(chars);
        ClassTag<Tuple2<Integer, Character>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualZipOutput), tag),
                fromRDD(toRDD(expectedZipOutput), tag));
    }

    @Test
    public void zipWithIndexTest() {
        JavaRDD<Character> chars = jsc().parallelize(asList('a', 'b', 'c', 'd', 'e'));
        JavaPairRDD<Character, Long> expectedZipWithIndexOutput = jsc().parallelizePairs(asList(
                new Tuple2<>('a', 0L),
                new Tuple2<>('b', 1L),
                new Tuple2<>('c', 2L),
                new Tuple2<>('d', 3L),
                new Tuple2<>('e', 4L)));
        JavaPairRDD<Character, Long> actualZipWithIndexOutput = chars.zipWithIndex();
        ClassTag<Tuple2<Character, Long>> tag = ClassTag$.MODULE$.apply(Tuple2.class);
        JavaRDDComparisons.assertRDDEquals(
                fromRDD(toRDD(actualZipWithIndexOutput), tag),
                fromRDD(toRDD(expectedZipWithIndexOutput), tag));
    }
}
