package com.nayanzin.highperformancespark.ch2howsparkworks;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;

import static com.nayanzin.highperformancespark.ch2howsparkworks.WordCount.countWords;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class WordCountTest extends SharedJavaSparkContext implements Serializable {

    @Test
    public void test() {
        JavaRDD<String> lines = jsc().parallelize(asList(
                "Hello world", "Hello Spark", "hello High-Performance spark"));

        JavaPairRDD<String, Long> actualWordsWithCounts = countWords(lines);

        assertThat(actualWordsWithCounts.collectAsMap()).containsOnly(
                entry("hello", 3L),
                entry("world", 1L),
                entry("spark", 2L),
                entry("high-performance", 1L));
    }
}