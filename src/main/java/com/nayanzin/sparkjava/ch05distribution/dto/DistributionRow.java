package com.nayanzin.sparkjava.ch05distribution.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DistributionRow {
    private int rangeNum;
    private double probability;
}
