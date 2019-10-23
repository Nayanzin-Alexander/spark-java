package com.nayanzin.sparkjava.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Common {

    public static JavaSparkContext buildSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Word count");
        return new JavaSparkContext(conf);
    }

    public static void checkArguments(String[] args) {
        // Check arguments
        if (args.length < 2) {
            System.err.println("Usage: <path-to-source-file> <path-to-result-file>");
            System.exit(-1);
        }
    }
}
