package com.nayanzin.sparkjava.ch01basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Basics {

    public static void main(String[] args) {
        checkArguments(args);
        JavaSparkContext sc = buildSparkContext();

        JavaRDD<String> input = sc.textFile(args[0]);

        long count = input.count();
    }

    private static JavaSparkContext buildSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Word count");
        return new JavaSparkContext(conf);
    }

    private static void checkArguments(String[] args) {
        // Check arguments
        if (args.length < 2) {
            System.err.println("Usage: <path-to-source-file> <path-to-result-file>");
            System.exit(-1);
        }
    }
}
