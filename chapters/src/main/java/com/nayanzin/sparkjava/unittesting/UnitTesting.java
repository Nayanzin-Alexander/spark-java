package com.nayanzin.sparkjava.unittesting;

import com.nayanzin.sparkjava.common.Common;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

import static com.nayanzin.sparkjava.common.Common.buildSparkContext;

public class UnitTesting {

    public static void main(String... args) {
        Common.checkArguments(args);
        JavaSparkContext sc = buildSparkContext();

        // Load input data
        final JavaRDD<String> input = sc.textFile(args[0]);

        // Split into words and numbers
        JavaPairRDD<String, Integer> wordsCount = getWordsCount(input);

        // Save results to the file
        wordsCount.saveAsTextFile(args[1]);
    }

    static JavaPairRDD<String, Integer> getWordsCount(JavaRDD<String> input) {
        return input
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);
    }
}
