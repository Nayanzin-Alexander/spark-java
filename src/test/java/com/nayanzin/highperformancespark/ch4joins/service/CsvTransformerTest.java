package com.nayanzin.highperformancespark.ch4joins.service;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class CsvTransformerTest extends SharedJavaSparkContext implements Serializable {

    private CsvTransformer csvTransformer = new CsvTransformer();

    @Test
    public void pandaAddressTest() {
        JavaRDD<String> validPandaAddresses = jsc().parallelize(asList(
                "panda_id,panda_address,panda_name",
                "0,address 0,Andy",
                "1,address 1,Mandy",
                "2,address 2,Sandy"), 200);
        JavaPairRDD<Long, PandaAddress> addresses = csvTransformer.getAddresses(validPandaAddresses);
        assertThat(addresses.collectAsMap()).containsOnly(
                        entry(0L, new PandaAddress(0L, "address 0", "Andy", null)),
                        entry(1L, new PandaAddress(1L, "address 1", "Mandy", null)),
                        entry(2L, new PandaAddress(2L, "address 2", "Sandy", null)));
    }

    @Test
    public void pandaScoreTest() {
        JavaRDD<String> validPandaScores = jsc().parallelize(asList(
                "panda_id,score",
                "0,2.3242",
                "1,23.11",
                "2,4.23",
                "AA,0.23",
                "4,AA",
                "AA"), 200);
        JavaPairRDD<Long, PandaScore> addresses = csvTransformer.getScores(validPandaScores);
        assertThat(addresses.collectAsMap()).containsOnly(
                entry(0L, new PandaScore(0L, 2.3242D, null)),
                entry(1L, new PandaScore(1L,23.11D, null)),
                entry(2L, new PandaScore(2L,4.23D, null)),
                entry(null, new PandaScore(null,0.23D, format("Failed to parse id: %s", "AA"))),
                entry(4L, new PandaScore(4L,null, format("Failed to parse score: %s", "AA"))),
                entry(null, new PandaScore(null,null, format("Wrong cols number in line: %s", "AA"))));
    }
}