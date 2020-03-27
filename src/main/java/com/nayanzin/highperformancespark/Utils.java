package com.nayanzin.highperformancespark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Objects.nonNull;

public final class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final String CALL_SITE_SHORT = "callSite.short";

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

    public static void setStageName(JavaSparkContext sc, String name) {
        sc.setLocalProperty(CALL_SITE_SHORT, name);
    }

    public static void checkArgs(String[] args, int expectedNumberOfArgs) {
        if (args.length != expectedNumberOfArgs) {
            LOG.error("Wrong arguments number. Must be {} but provided: {}", expectedNumberOfArgs, args.length);
            System.exit(-1);
        }
    }
}
