package com.nayanzin.highperformancespark.ch4joins.filter;

import scala.Tuple2;

import static org.apache.commons.lang3.ObjectUtils.allNotNull;

public class Filters {
    private Filters() {}

    public static boolean filterCsvHeader(Tuple2<String, Long> csvLineWithIndex) {
        return csvLineWithIndex._2 > 0;
    }

    public static Boolean filterIdNameAddress(Tuple2<Long, Tuple2<String, String>> idNameScore) {
        return allNotNull(idNameScore, idNameScore._1, idNameScore._2, idNameScore._2._1, idNameScore._2._2);
    }

    public static Boolean filterIdScore(Tuple2<Long, Double> idScore) {
        return allNotNull(idScore, idScore._1, idScore._2);
    }

    public static boolean filterTuple2NonNullKeyValue(Tuple2<?,?> tuple2) {
        return allNotNull(tuple2, tuple2._1, tuple2._2);
    }
}
