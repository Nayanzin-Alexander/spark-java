package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.filter.Filters;
import com.nayanzin.highperformancespark.ch4joins.mapper.Mappers;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;

import static com.nayanzin.highperformancespark.Utils.buildSparkSession;
import static com.nayanzin.highperformancespark.Utils.setStageName;

/*
Usage:
./gradlew build

INPUT_ADDRESS=/home/fin/MyProjects/spark-java/src/test/resources/highperformancespark/ch4joins/addresses_35mb.csv
INPUT_SCORES=/home/fin/MyProjects/spark-java/src/test/resources/highperformancespark/ch4joins/scores_300mb.csv
OUTPUT=/home/fin/MyProjects/spark-java/manual-run-results/CoreRddJoin/
rm -r $OUTPUT
spark-submit    \
    --name Reduce-Join \
    --total-executor-cores 4  \
    --class com.nayanzin.highperformancespark.ch4joins.Join  \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT_ADDRESS $INPUT_SCORES $OUTPUT
 */
public class Join implements Serializable {

    public static void main(String[] args) {
        // Parse arguments
        checkArgs(args);
        String inputAddressCsv = args[0];
        String inputScoreCsv = args[1];
        String outputAddressWithHighestScore = args[2];

        // Create spark session
        SparkSession spark = buildSparkSession(null, Collections.emptyMap());
        SparkContext sc = spark.sparkContext();

        setStageName(sc, "Read input address csv files");
        JavaPairRDD<Long, Tuple2<String, String>> idNameAddress = sc
                .textFile(inputAddressCsv, 1)
                .toJavaRDD()
                .mapToPair(Mappers::csvLineToIdNameAddressOrNull)
                .filter(Filters::filterIdNameAddress);

        setStageName(sc, "Read input score files");
        JavaPairRDD<Long, Double> idScore = sc
                .textFile(inputScoreCsv, 1)
                .toJavaRDD()
                .mapToPair(Mappers::csvLineToIdScore)
                .filter(Filters::filterIdScore);

        setStageName(sc, "Reduce scores get max score by id, join with address and name and save");
        idScore
                .reduceByKey(Double::max)
                .join(idNameAddress)
                .map(Mappers::idMaxScoreNameAddressToCsvLine)
                .saveAsTextFile(outputAddressWithHighestScore);
    }


    private static void checkArgs(String[] args) {
        if (args.length != 3) {
            System.exit(-1);
        }
    }
}
