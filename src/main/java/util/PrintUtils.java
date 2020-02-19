package util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class PrintUtils {
    public static void println(Object line) {
        System.out.println(line);
    }

    public static void printH1(String header) {
        System.out.println("\n\n\n");
        System.out.println(header);
        System.out.println("\n");
    }

    public static void printH2(String header) {
        System.out.println("\n");
        System.out.println(header);
    }

    public static <T> void printRDD(JavaRDD<T> rdd) {
        rdd.foreach(PrintUtils::println);
    }

    public static <K, V> void printRDD(JavaPairRDD<K,V> rdd) {
        rdd.foreach(PrintUtils::println);
    }
}
