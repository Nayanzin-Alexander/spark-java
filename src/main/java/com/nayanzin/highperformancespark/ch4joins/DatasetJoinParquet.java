package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaScore;
import com.nayanzin.highperformancespark.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static com.nayanzin.highperformancespark.ch4joins.service.DatasetJoiner.joinAddressesWithMaxScore;
import static java.util.Collections.emptyMap;

/*
Usage:
INPUT_ADDRESS=src/test/resources/highperformancespark/ch4joins/parquet_addresses
INPUT_SCORES=src/test/resources/highperformancespark/ch4joins/parquet_scores
OUTPUT=manual-run-results/DatasetJoinParquetPartitioned/
rm -r $OUTPUT
spark-submit \
    --master    local[4] \
    --name      DatasetJoinParquet \
    --class     com.nayanzin.highperformancespark.ch4joins.DatasetJoinParquet  \
    --conf="spark.driver.memory=5G" \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT_ADDRESS \
    $INPUT_SCORES \
    $OUTPUT
 */
public class DatasetJoinParquet {
    public static void main(String[] args) {
        SparkSession spark = Utils.buildSparkSession("DatasetJoinParquet", emptyMap());
        Dataset<DatasetPandaAddress> addresses = spark.read()
                .parquet(args[0])
                .as(Encoders.bean(DatasetPandaAddress.class));

        Dataset<DatasetPandaScore> scores = spark.read()
                .parquet(args[1])
                .as(Encoders.bean(DatasetPandaScore.class));

        Dataset<DatasetPandaCongratsDto> congrats = joinAddressesWithMaxScore(addresses, scores);
        congrats
                .write()
                .partitionBy("partition")
                .parquet(args[2]);
    }
}
