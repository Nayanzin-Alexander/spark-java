package com.nayanzin.sparkjava.ch01basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import util.SparkSessionHolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.Spliterators.spliteratorUnknownSize;
import static util.JavaToSparkConverter.parallelize;
import static util.PrintUtils.*;

public class RDDNarrowTransformations {

    public static void main(String[] args) {
        SparkSession spark = SparkSessionHolder.getSparkSession();

        String text = "Hello world \nHello world again from the second line.";
        JavaRDD<String> words = parallelize(text.split(" "), 4);

        // runNarrowTransformationExamples(words);
        runWideTransformationExamples(words);

        spark.stop();
    }

    private static void runWideTransformationExamples(JavaRDD<String> words) {

        // todo https://courses.epam.com/courses/course-v1:EPAM+101BD+0819/courseware/72a6f77779194d0bb84fc0e5b1c82c75/a8a1ad22879946fc8c3fdecd57b9559e/1
        printH2("intersection(dataset)");
    }

    private static void runNarrowTransformationExamples(JavaRDD<String> words) {
        printH1("Narrow Transformations");


        printH2("map(func)");
        printRDD(words.map(String::toUpperCase));


        printH2("flatMap(func)");
        JavaRDD<String> chars = words
                .flatMap(word -> Arrays
                        .asList(word.split(""))
                        .iterator());
        printRDD(chars);


        printH2("filter(func)");
        printRDD(words.filter("Hello"::equals));


        printH2("union(dataset)");
        printRDD(words.union(words));


        printH2("mapPartition(func)");
        JavaRDD<Integer> wordsCountInEachPartition = words.mapPartitions(iterator -> {
            int wordsCount = 0;
            while (iterator.hasNext()) {
                wordsCount++;
                iterator.next();
            }
            return Collections.singletonList(wordsCount).iterator();
        });
        printRDD(wordsCountInEachPartition);


        printH2("mapPartitionWithIndex(func)");
        JavaRDD<String> wordsCountInEachPartitionWithPartitionIndex = words.mapPartitionsWithIndex((index, iterator) -> {
            println("Inside paritition with index: " + index);
            int wordCounter = 0;
            while (iterator.hasNext()) {
                wordCounter++;
                iterator.next();
            }
            return Collections.singletonList(format("Partition with index [%d] has %d words", index, wordCounter))
                    .iterator();
        }, true);
        printRDD(wordsCountInEachPartitionWithPartitionIndex);


        JavaRDD<String> wordsWithPartitionIndex = words
                .mapPartitionsWithIndex((index, iterator) ->
                        StreamSupport.stream(spliteratorUnknownSize(iterator, 0), false)
                                .map(word -> format("[%d] %s", index, word))
                                .iterator(), true);
        printRDD(wordsWithPartitionIndex);


        printH2("zip(dataset)");
        JavaRDD<Integer> someNumbers = parallelize(new Integer[] {1, 2, 3}, 4);
        JavaRDD<Character> someChars = parallelize(new Character[] {'a', 'b', 'c'}, 4);
        JavaPairRDD<Integer, Character> zipRdd = someNumbers.zip(someChars);
        printRDD(zipRdd);


        printH2("zipWithIndex()");
        JavaPairRDD<Character, Long> zipWithIndex = someChars.zipWithIndex();
        printRDD(zipWithIndex);


        printH2("zipWithUniqueId()");
        JavaPairRDD<String, Long> wordsWithUniqId = words.zipWithUniqueId();
        printRDD(wordsWithUniqId);
    }

}
