package com.nayanzin.sparkjava.ch01basics;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.nayanzin.sparkjava.ch01basics.TestUtils.deleteFiles;
import static com.nayanzin.sparkjava.ch01basics.TestUtils.getTestResource;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Output files are writen to build/resources/test/{testPackage}/{testClassName}/{methodName}
 */
public class RDDSaveTest extends SharedJavaSparkContext implements Serializable {
    private static final Path inputFile = getTestResource("/ch01basics/testData.txt");
    private static final String outputDir = getTestResource("/ch01basics") + "/RDDSaveTest/";

    @BeforeClass
    public static void cleanUpPreviousGeneratedFiles() throws IOException {
        deleteFiles(Paths.get(outputDir));
    }

    @Test
    public void saveAsTextFileTest() {
        String destination = outputDir + "/saveAsTextFileTest/";
        jsc().textFile(inputFile.toString(), 2)
                .saveAsTextFile(destination);
        assertThat(Paths.get(destination)).isDirectory();
        assertThat(Paths.get(destination + "/_SUCCESS")).exists();
    }

    @Test
    public void saveAsSequenceFileTest() {
        String destination = outputDir + "/saveAsSequenceFileTest/";
        jsc().textFile(inputFile.toString(), 2)
                .mapToPair(line -> new Tuple2<>(new LongWritable(line.hashCode()), new Text(line)))
                .saveAsHadoopFile(destination, LongWritable.class, Text.class, SequenceFileOutputFormat.class);
        assertThat(Paths.get(destination)).isDirectory();
        assertThat(Paths.get(destination + "/_SUCCESS")).exists();
    }
}
