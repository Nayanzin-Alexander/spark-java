package com.nayanzin.highperformancespark.ch2howsparkworks;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import static com.nayanzin.sparkjava.util.TestUtils.getTestResource;
import static org.assertj.core.api.Assertions.assertThat;

public class RddPropertiesTest extends SharedJavaSparkContext implements Serializable {
    private static final Path inputFile = getTestResource("/highperformancespark/ch2howsparkworks/testData.txt");

    @Test
    public void rddPropertiesTest() {
        int minPartitions = 10;
        JavaRDD<String> rdd = jsc().textFile(inputFile.toString(), minPartitions);

        /*
        Returns an array of the partition objects that make up the parts of the distributed dataset.
         */
        List<Partition> partitions = rdd.partitions();
        assertThat(partitions.size()).isGreaterThanOrEqualTo(minPartitions);

        /*
        Returns a function between element and partition associated with it, such as hashPartitioner.
         */
        Partitioner partitioner = rdd.partitioner().orNull();
        assertThat(partitioner).isNull();

        String rddType = rdd.toDebugString();
        assertThat(rddType).contains("MapPartitionsRDD", "HadoopRDD");
    }
}
