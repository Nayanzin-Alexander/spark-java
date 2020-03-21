package com.nayanzin.highperformancespark.ch4joins.filter;

import scala.Tuple2;

public class Filters {
    private Filters() {}

    public static boolean filterCsvHeader(Tuple2<String, Long> csvLineWithIndex) {
        return csvLineWithIndex._2 > 0;
    }
}
