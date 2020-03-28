package com.nayanzin.sparkjava.ch04partitions;

import com.nayanzin.highperformancespark.Utils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/*
ROWS=129870120
ITERATIONS=100
OUTPUT=manual-run-results/GenerateParquet/
spark-submit \
    --master    local[4] \
    --name      GenerateParquet \
    --class     com.nayanzin.sparkjava.ch04partitions.GenerateParquet  \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $ROWS \
    $ITERATIONS \
    $OUTPUT
 */
public class GenerateParquet {

    private static HashPartitioner hashPartitioner = new HashPartitioner(10);

    public static void main(String[] args) {
        SparkSession sparkSession = Utils.buildSparkSession(null, Collections.emptyMap());
        int rows = Integer.parseInt(args[0]);
        int iterations = Integer.parseInt(args[1]);
        int rowsPerBuffer = rows / iterations + rows % iterations;
        for (int i = 0; i < iterations; i++) {
            int startInclusive = i * rowsPerBuffer;
            int endExclusive = Integer.min(rows, startInclusive + rowsPerBuffer);
            List<Dto> data = generateData(startInclusive, endExclusive);
            Dataset<Dto> dataset = sparkSession.createDataset(data, Encoders.bean(Dto.class));
            dataset.write()
                    .partitionBy("partition")
                    .mode("append")
                    .parquet(args[2]);
        }
    }

    private static List<Dto> generateData(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> Dto.builder()
                        .id((long) i)
                        .partition(hashPartitioner.getPartition((long) i))
                        .text("Text_" + i)
                        .build())
                .collect(toList());
    }
}
