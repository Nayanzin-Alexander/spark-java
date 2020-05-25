package com.nayanzin.highperformancespark.ch4joins;

import com.nayanzin.highperformancespark.ch4joins.filter.Filters;
import com.nayanzin.highperformancespark.ch4joins.mapper.Mappers;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;
import java.util.Map;

import static com.nayanzin.highperformancespark.utils.Utils.buildSparkSession;
import static com.nayanzin.highperformancespark.utils.Utils.setStageName;
import static java.util.Objects.isNull;

/*
Usage:
INPUT_ADDRESS=src/test/resources/highperformancespark/ch4joins/addresses_70mb.csv
INPUT_SCORES=src/test/resources/highperformancespark/ch4joins/scores_300mb.csv
OUTPUT=manual-run-results/CoreRddJoin/
rm -r $OUTPUT
spark-submit \
    --master    local[4] \
    --name      RddCoLocatedJoin \
    --class     com.nayanzin.highperformancespark.ch4joins.RddCoLocatedJoin  \
    --conf="spark.driver.memory=5G" \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT_ADDRESS \
    $INPUT_SCORES \
    $OUTPUT
 */
public class RddCoLocatedJoin implements Serializable {

    public static void main(String[] args) {
        // Parse arguments
        checkArgs(args);
        String inputAddressCsv = args[0];
        String inputScoreCsv = args[1];
        String outputAddressWithHighestScore = args[2];

        // Create spark session
        SparkSession spark = buildSparkSession(null, Collections.emptyMap());
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        setStageName(sc, "Read input address csv files and broadcast them");
        Map<Long, Tuple2<String, String>> idNameAddress = sc
                .textFile(inputAddressCsv, 1)
                .mapToPair(Mappers::csvLineToIdNameAddressOrNull)
                .filter(Filters::filterIdNameAddress)
                .collectAsMap();
        Broadcast<Map<Long, Tuple2<String, String>>> nameAddressBroadcast = sc.broadcast(idNameAddress);

        setStageName(sc, "Read input score files");
        JavaPairRDD<Long, Double> idScore = sc
                .textFile(inputScoreCsv, 1)
                .mapToPair(Mappers::csvLineToIdScore)
                .filter(Filters::filterIdScore);

        setStageName(sc, "Reduce scores get max score by id, join with address and name broadcast and save");
        idScore.reduceByKey(Double::max)
                .map(t -> {
                    Tuple2<String, String> addressName = nameAddressBroadcast.getValue().get(t._1);
                    return isNull(addressName)
                            ? new Tuple2<Long, Tuple2<Double, Tuple2<String, String>>>(null, null)
                            : new Tuple2<>(t._1, new Tuple2<>(t._2, addressName));
                })
                .filter(Filters::filterTuple2NonNullKeyValue)
                .map(Mappers::idMaxScoreNameAddressToCsvLine)
                .saveAsTextFile(outputAddressWithHighestScore);
    }


    private static void checkArgs(String[] args) {
        if (args.length != 3) {
            System.exit(-1);
        }
    }
}
