package util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionHolder {

    private static final SparkSession sparkSession = SparkSession
            .builder()
            .appName("Simple Application")
            .getOrCreate();

    public static SparkSession getSparkSession() {
        return sparkSession;
    }

}
