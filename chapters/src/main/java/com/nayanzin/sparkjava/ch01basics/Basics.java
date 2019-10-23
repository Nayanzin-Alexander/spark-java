package com.nayanzin.sparkjava.ch01basics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static com.nayanzin.sparkjava.common.Common.buildSparkContext;
import static com.nayanzin.sparkjava.common.Common.checkArguments;

public class Basics {

    public static void main(String[] args) {
        checkArguments(args);
        JavaSparkContext sc = buildSparkContext();

        JavaRDD<String> input = sc.textFile(args[0]);

        long count = input.count();
    }
}
