package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ConvertRddToDataset extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("name", StringType, false),
            createStructField("number", IntegerType, false)));

    @Test
    public void countByKeyTest() {
        JavaPairRDD<String, Integer> pairRdd = jsc().parallelizePairs(asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("A", 11),
                new Tuple2<>("A", 111),
                new Tuple2<>("B", 2),
                new Tuple2<>("B", 22),
                new Tuple2<>("C", 3)));
        JavaRDD<Row> rdd = pairRdd.map(tuple -> create(tuple._1, tuple._2));
        Dataset<Row> dataset = spark().createDataFrame(rdd, SCHEMA);
        assertThat(dataset.collectAsList()).containsExactlyInAnyOrder(
                create("A", 1),
                create("A", 11),
                create("A", 111),
                create("B", 2),
                create("B", 22),
                create("C", 3)
        );
    }
}
