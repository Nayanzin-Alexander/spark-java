package com.nayanzin.highperformancespark.ch4joins.service;

import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.DatasetPandaScore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class DatasetJoiner {

    public static final String SCORE_ID = "score_id";

    private DatasetJoiner() {}
    public static Dataset<DatasetPandaCongratsDto> joinAddressesWithMaxScore(Dataset<DatasetPandaAddress> addresses, Dataset<DatasetPandaScore> scores) {
        return scores
                .groupBy(col("id").as(SCORE_ID))
                .agg(max(col("score")).as("highestScore"))
                .join(addresses, col("SCORE_ID").equalTo(addresses.col("id")))
                .drop(col("SCORE_ID"))
                .as(Encoders.bean(DatasetPandaCongratsDto.class));
    }
}
