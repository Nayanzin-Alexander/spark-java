package com.nayanzin.sparkjava.ch03dataset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AgeGroup implements Serializable {
    private Integer ageLowerBound;  // including
    private Integer ageUpperBound; // not-including
    private String info;
}