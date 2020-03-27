package com.nayanzin.highperformancespark.ch4joins.service;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import com.nayanzin.highperformancespark.ch4joins.filter.Filters;
import com.nayanzin.highperformancespark.ch4joins.mapper.Mappers;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class RddCsvTransformer {
    public JavaPairRDD<Long, PandaScore> getScores(JavaRDD<String> addressCsvRdd) {
        return addressCsvRdd
                .zipWithIndex()
                .filter(Filters::filterCsvHeader)
                .mapToPair(lineWithIndexTuple2 -> {
                    PandaScore score = Mappers.csvToPandaScore(lineWithIndexTuple2._1);
                    return new Tuple2<>(score.getId(), score);
                });
    }

    public JavaPairRDD<Long, PandaAddress> getAddresses(JavaRDD<String> addressCsvRdd) {
        return addressCsvRdd
                .zipWithIndex()
                .filter(Filters::filterCsvHeader)
                .mapToPair(lineWithIndexTuple2 -> {
                    PandaAddress pandaAddress = Mappers.csvToPandaAddress(lineWithIndexTuple2._1);
                    return new Tuple2<>(pandaAddress.getId(), pandaAddress);
                });
    }
}
