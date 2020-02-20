package com.nayanzin.sparkjava.ch01basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/*
## Usage:
INPUT=src/test/resources/ch01basics/testData.txt
OUTPUT=src/test/resources/ch01basics/result/
rm -r $OUTPUT
spark-submit    \
    --class com.nayanzin.sparkjava.ch01basics.WordCount \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT $OUTPUT
 */
public class WordCount {

    public static void main(String... args) {

        checkArguments(args);

        JavaSparkContext sc = buildSparkContext();
        final JavaRDD<String> input = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> wordsCount = getWordsCount(input);
        wordsCount.saveAsTextFile(args[1]);
    }

    public static JavaPairRDD<String, Integer> getWordsCount(JavaRDD<String> input) {
        return input
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);
    }

    private static JavaSparkContext buildSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Word count");
        return new JavaSparkContext(conf);
    }

    @SuppressWarnings("squid:S106")
    private static void checkArguments(String[] args) {
        // Check arguments
        if (args.length < 2) {
            System.err.println("Usage: <path-to-source-file> <path-to-result-file>");
            System.exit(-1);
        }
    }
}
