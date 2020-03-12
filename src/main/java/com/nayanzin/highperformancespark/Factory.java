package com.nayanzin.highperformancespark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public final class Factory {

    private Factory() {}

    public static SparkSession buildSparkSession(String appName, Map<String, String> sparkConf) {
        SparkConf conf = new SparkConf();
        sparkConf.forEach(conf::set);
        return SparkSession
                .builder()
                .enableHiveSupport()
                .config(conf)
                .appName(appName)
                .getOrCreate();
    }
}
