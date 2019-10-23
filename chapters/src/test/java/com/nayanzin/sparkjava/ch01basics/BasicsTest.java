package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Arrays.asList;
import static java.util.Comparator.reverseOrder;
import static org.assertj.core.api.Assertions.assertThat;

public class BasicsTest extends SharedJavaSparkContext {

    private static Path testResources = new File(BasicsTest.class.getResource("/ch01basics").getFile()).toPath();
    private static Path testFilesDir = Paths.get(testResources.toString(), "/testFiles");
    private JavaRDD<String> inputRDD;
    private JavaPairRDD<String, Integer> wordsWithLength;

    @AfterClass
    public static void cleanUpTestFiles() throws IOException {
        int filesDeleted = Files.walk(testFilesDir)
                .sorted(reverseOrder())
                .map(Path::toFile)
                .mapToInt(file -> file.delete() ? 1 : 0)
                .sum();
        assertThat(filesDeleted).as("Files deleted").isGreaterThan(2);
    }

    @Before
    public void setup() {
        inputRDD = jsc().textFile(testResources + "/testData.txt");
        wordsWithLength = inputRDD.map(String::trim)
                .filter(str -> !str.isEmpty())
                .flatMap(line -> asList(line.split(" ")).iterator())
                .map(line -> line.replaceAll("\\W", ""))
                .mapToPair(word -> new Tuple2<>(word, word.length()));
    }

    @Test
    public void basicTransformationsTest() {
        // Filtering
        long notEmptyRowsCount = inputRDD
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .count();
        assertThat(notEmptyRowsCount).as("Not empty rows count").isEqualTo(11L);

        // Map
        int lengthSum = inputRDD
                .map(String::length)
                .reduce(Integer::sum);
        assertThat(lengthSum).as("Line length sum").isEqualTo(565);

        // FlatMap
        long wordCount = inputRDD
                .map(String::trim)
                .flatMap(str -> asList(str.split(" ")).iterator())
                .count();
        assertThat(wordCount).as("Words count").isEqualTo(68L);

        // Union
        JavaRDD<Integer> rdd1 = jsc().parallelize(asList(1, 2, 3, 4), 4);
        JavaRDD<Integer> rdd2 = jsc().parallelize(asList(3, 4, 5, 6), 4);
        JavaRDD<Integer> rddUnion = rdd1.union(rdd2);
        assertThat(rddUnion.collect()).as("Union of two RDDs")
                .containsExactly(1, 2, 3, 4, 3, 4, 5, 6);

        // Distinct
        JavaRDD<Integer> rddDistinct = rddUnion.distinct();
        assertThat(rddDistinct.collect()).as("Rdd distinct")
                .containsExactly(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void basicActionsTest() {
        // Count
        long rowCount = inputRDD.count();
        assertThat(rowCount).as("Row count").isEqualTo(14L);

        // Collect
        JavaRDD<Integer> rdd = jsc().parallelize(asList(1, 2, 3, 4, 5));
        assertThat(rdd.collect()).as("RDD collect")
                .containsExactly(1, 2, 3, 4, 5);

        // Reduce
        int reducedMultiply = rdd.reduce((a, b) -> a * b);
        assertThat(reducedMultiply).as("Reduce multiply").isEqualTo(120);

        // Foreach
        rdd.foreach(element -> assertThat(element).as("One RDD element").isPositive());
    }

    @Test
    public void pairedRddTransformationsTest() {
        assertThat(wordsWithLength.collect()).as("Paired word and it's length")
                .contains(new Tuple2<>("for", 3),
                        new Tuple2<>("dataset", 7),
                        new Tuple2<>("DataFrameSuiteBase", 18));

        // reduceByKey
        JavaPairRDD<String, Integer> wordsLengthSum = wordsWithLength
                .reduceByKey(Integer::sum);
        assertThat(wordsLengthSum.collect()).as("Words length sum")
                .contains(new Tuple2<>("for", 6),
                        new Tuple2<>("two", 3),
                        new Tuple2<>("dataset", 7),
                        new Tuple2<>("Generates", 27));

        // GroupByKey
        JavaPairRDD<String, Iterable<Integer>> wordAndLengths = wordsWithLength.groupByKey();
        wordAndLengths.collectAsMap()
                .forEach((key, value) -> {
                    switch (key) {
                        case "equality":
                            assertThat(value).containsExactly(8, 8);
                            break;
                        case "two":
                            assertThat(value).containsExactly(3);
                            break;
                        case "arbitrary":
                        case "Generates":
                            assertThat(value).containsExactly(9, 9, 9);
                            break;
                    }
                });
    }

    @Test
    public void savingDataTest() {
        // Saving as text file
        String txtFile = testFilesDir.toString() + "/savingDataTest1.txt";
        inputRDD.saveAsTextFile(txtFile);
        assertThat(Paths.get(txtFile)).isDirectory();
        assertThat(Paths.get(txtFile + "/_SUCCESS")).exists();

        String sequenceFile = testFilesDir.toString() + "/savingDataTest1.sequence";
        inputRDD.saveAsTextFile(sequenceFile);
        assertThat(Paths.get(sequenceFile)).isDirectory();
        assertThat(Paths.get(sequenceFile + "/_SUCCESS")).exists();
    }
}