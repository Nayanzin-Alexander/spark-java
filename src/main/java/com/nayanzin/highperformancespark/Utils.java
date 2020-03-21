package com.nayanzin.highperformancespark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static java.util.Objects.nonNull;

public final class Utils {

    public static final String CALL_SITE_SHORT = "callSite.short";

    private Utils() {}

    public static SparkSession buildSparkSession(String appName, Map<String, String> sparkConf) {
        SparkConf conf = new SparkConf();
        sparkConf.forEach(conf::set);
        if (nonNull(appName)) {
            conf.set("spark.app.name", appName);
        }
        return SparkSession
                .builder()
                .enableHiveSupport()
                .config(conf)
                .getOrCreate();
    }

    public static void setStageName(SparkContext sc, String name) {
        sc.setLocalProperty(CALL_SITE_SHORT, name);
    }
}
