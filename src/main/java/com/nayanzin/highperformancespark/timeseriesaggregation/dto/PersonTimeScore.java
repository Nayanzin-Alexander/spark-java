package com.nayanzin.highperformancespark.timeseriesaggregation.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class PersonTimeScore implements Serializable {
    private String name;
    private long hour;
    private int score;
}
