package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.filter.Filters;
import com.nayanzin.highperformancespark.ch4joins.mapper.Mappers;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;

import static com.nayanzin.highperformancespark.Utils.*;

/*
Usage:
INPUT_ADDRESS=src/test/resources/highperformancespark/ch4joins/addresses_70mb.csv
INPUT_SCORES=src/test/resources/highperformancespark/ch4joins/scores_300mb.csv
OUTPUT=manual-run-results/CoreRddJoin/
rm -r $OUTPUT
spark-submit \
    --master    local[4] \
    --name      RddKnownPartitionersJoin \
    --class     com.nayanzin.highperformancespark.ch4joins.RddKnownPartitionersJoin \
    build/libs/spark-java-1.0-SNAPSHOT.jar \
    $INPUT_ADDRESS \
    $INPUT_SCORES \
    $OUTPUT
 */
public class RddKnownPartitionersJoin implements Serializable {

    public static void main(String[] args) {
        // Parse arguments
        checkArgs(args, 3);
        String inputAddressCsv = args[0];
        String inputScoreCsv = args[1];
        String outputAddressWithHighestScore = args[2];

        // Create spark session
        SparkSession spark = buildSparkSession(null, Collections.emptyMap());
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        setStageName(sc, "Read input address csv files");
        JavaPairRDD<Long, Tuple2<String, String>> idNameAddress = sc
                .textFile(inputAddressCsv, 10)
                .mapToPair(Mappers::csvLineToIdNameAddressOrNull)
                .filter(Filters::filterIdNameAddress);

        setStageName(sc, "Read input score files");
        JavaPairRDD<Long, Double> idScore = sc
                .textFile(inputScoreCsv, 10)
                .mapToPair(Mappers::csvLineToIdScore)
                .filter(Filters::filterIdScore);

        // Get idNameAddress partitioner
        Partitioner addressPartitioner = idNameAddress.partitioner()
                .or(new HashPartitioner(idNameAddress.partitions().size()));

        setStageName(sc, "Reduce scores get max score by id, join with address and name and save");
        idScore
                .reduceByKey(addressPartitioner, Double::max)
                .join(idNameAddress)
                .map(Mappers::idMaxScoreNameAddressToCsvLine)
                .saveAsTextFile(outputAddressWithHighestScore);
    }
}
