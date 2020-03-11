package com.nayanzin.highperformancespark.ch2howsparkworks;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/*
## Usage:
./gradlew build

## For Spark debugging
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=7777

INPUT=src/test/resources/ch01basics/testData.txt
OUTPUT=manual-run-results/wordcount/
rm -r $OUTPUT
spark-submit    \
    --class com.nayanzin.highperformancespark.ch2howsparkworks.WordCount \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT $OUTPUT
 */
public class WordCount implements Serializable {

    public static void main(String[] args) {
        checkArguments(args);
        String input = args[0];
        String output = args[1];

        JavaSparkContext jsc = buildSparkContext();
        JavaRDD<String> lines = jsc.textFile(input);

        JavaPairRDD<String, Long> wordWithCounts = countWords(lines);

        wordWithCounts.saveAsTextFile(output);

        jsc.stop();
    }

    public static JavaPairRDD<String, Long> countWords(JavaRDD<String> lines) {
        return lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(String::toLowerCase)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum);
    }

    private static JavaSparkContext buildSparkContext() {
        SparkConf conf = new SparkConf()
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
