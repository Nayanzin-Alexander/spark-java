package com.nayanzin.highperformancespark.ch4joins.service;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import com.nayanzin.highperformancespark.ch4joins.mapper.Mappers;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;

import static java.util.Objects.isNull;

public class JoinTransformer implements Serializable {
    public JavaRDD<PandaCongratsDto> joinReduce(JavaPairRDD<Long, PandaAddress> addresses, JavaPairRDD<Long, PandaScore> scores) {
        return scores.join(addresses)
                .mapToPair(t -> new Tuple2<>(t._1, Mappers.addressAndScoreToPandaCongratsDto(t._2._2, t._2._1)))
                .reduceByKey((t1, t2) -> {
                    if (isNull(t1.getHighestScore())) {
                        return t2;
                    }
                    return t1.getHighestScore().compareTo(t2.getHighestScore()) > 0 ? t1 : t2;
                })
                .map(t -> t._2);
    }

    public JavaRDD<PandaCongratsDto> reduceJoin(JavaPairRDD<Long, PandaAddress> addresses, JavaPairRDD<Long, PandaScore> scores) {
        return scores
                .reduceByKey((t1, t2) -> {
                    if (isNull(t1.getScore())) {
                        return t2;
                    }
                    return t1.getScore().compareTo(t2.getScore()) > 0 ? t1 : t2;
                })
                .join(addresses)
                .map(t -> Mappers.addressAndScoreToPandaCongratsDto(t._2._2, t._2._1));
    }
}
