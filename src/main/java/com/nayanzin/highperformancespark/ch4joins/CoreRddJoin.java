package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import com.nayanzin.highperformancespark.ch4joins.service.CsvTransformer;
import com.nayanzin.highperformancespark.ch4joins.service.JoinTransformer;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

import java.util.Collections;

import static com.nayanzin.highperformancespark.Utils.buildSparkSession;
import static com.nayanzin.highperformancespark.Utils.setStageName;

/*
Usage:
./gradlew build

JOIN_METHOD=reduceJoin1
INPUT_ADDRESS=/home/fin/MyProjects/spark-java/src/test/resources/highperformancespark/ch4joins/addresses_35mb.csv
INPUT_SCORES=/home/fin/MyProjects/spark-java/src/test/resources/highperformancespark/ch4joins/scores_300mb.csv
OUTPUT=/home/fin/MyProjects/spark-java/manual-run-results/CoreRddJoin/
rm -r $OUTPUT
spark-submit    \
    --name $JOIN_METHOD \
    --total-executor-cores 4  \
    --class com.nayanzin.highperformancespark.ch4joins.CoreRddJoin  \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT_ADDRESS $INPUT_SCORES $OUTPUT $JOIN_METHOD
 */
public class CoreRddJoin implements Serializable {

    public static void main(String[] args) {
        // Parse arguments
        checkArgs(args);
        String inputAddressCsv = args[0];
        String inputScoreCsv = args[1];
        String outputAddressWithHighestScore = args[2];
        String joinMethod = args[3];

        // Create spark session
        SparkSession spark = buildSparkSession(null, Collections.emptyMap());
        SparkContext sc = spark.sparkContext();

        // Create service objects
        CsvTransformer csvTransformer = new CsvTransformer();
        JoinTransformer joinTransformer = new JoinTransformer();

        // Read input csv files
        setStageName(sc, "Read input scores csv files");
        JavaRDD<String> addressesCsv = sc.textFile(inputAddressCsv, 1).toJavaRDD();

        setStageName(sc, "Read input scores csv files");
        JavaRDD<String> scoresCsv = sc.textFile(inputScoreCsv, 1).toJavaRDD();

        // Transform csv to Dto
        setStageName(sc, "Transform addresses to Dto");
        JavaPairRDD<Long, PandaAddress> addresses = csvTransformer.getAddresses(addressesCsv);

        setStageName(sc, "Transform scores to Dto");
        JavaPairRDD<Long, PandaScore> scores = csvTransformer.getScores(scoresCsv);

        // Join data
        JavaRDD<PandaCongratsDto> addressesAndHighestScores =
                "joinReduce".equals(joinMethod)
                        ? joinTransformer.joinReduce(addresses, scores) :
                        joinTransformer.reduceJoin(addresses, scores);

        // Save result
        setStageName(sc, "Join, save");
        addressesAndHighestScores
                .coalesce(1)
                .saveAsTextFile(outputAddressWithHighestScore);
    }


    private static void checkArgs(String[] args) {
        if (args.length != 4) {
            System.exit(-1);
        }
    }
}
