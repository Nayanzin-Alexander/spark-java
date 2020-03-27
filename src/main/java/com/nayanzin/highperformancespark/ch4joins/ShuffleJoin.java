package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import com.nayanzin.highperformancespark.ch4joins.service.CsvTransformer;
import com.nayanzin.highperformancespark.ch4joins.service.JoinTransformer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

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
    --name      ShuffleJoin \
    --class     com.nayanzin.highperformancespark.ch4joins.ShuffleJoin \
    build/libs/spark-java-1.0-SNAPSHOT.jar \
    $INPUT_ADDRESS \
    $INPUT_SCORES \
    $OUTPUT
 */
public class ShuffleJoin implements Serializable {
    public static void main(String[] args) {
        // Parse arguments
        checkArgs(args, 3);
        String inputAddressCsv = args[0];
        String inputScoreCsv = args[1];
        String outputAddressWithHighestScore = args[2];

        // Create spark session
        SparkSession spark = buildSparkSession(null, Collections.emptyMap());
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Create service objects
        CsvTransformer csvTransformer = new CsvTransformer();
        JoinTransformer joinTransformer = new JoinTransformer();

        // Read input csv files
        setStageName(sc, "Read input scores csv files");
        JavaRDD<String> addressesCsv = sc.textFile(inputAddressCsv, 1);

        setStageName(sc, "Read input scores csv files");
        JavaRDD<String> scoresCsv = sc.textFile(inputScoreCsv, 1);

        // Transform csv to Dto
        setStageName(sc, "Transform addresses to Dto");
        JavaPairRDD<Long, PandaAddress> addresses = csvTransformer.getAddresses(addressesCsv);

        setStageName(sc, "Transform scores to Dto");
        JavaPairRDD<Long, PandaScore> scores = csvTransformer.getScores(scoresCsv);

        // Join data and Save result
        setStageName(sc, "Join, save");
        joinTransformer.joinReduce(addresses, scores)
                .saveAsTextFile(outputAddressWithHighestScore);
    }
}
