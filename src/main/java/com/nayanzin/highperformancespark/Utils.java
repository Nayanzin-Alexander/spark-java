package com.nayanzin.highperformancespark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public final class Utils {

    public static final String CALL_SITE_SHORT = "callSite.short";

    private Utils() {}

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

    public static void setStageName(SparkContext sc, String name) {
        sc.setLocalProperty(CALL_SITE_SHORT, name);
    }
}
