package com.nayanzin.sparkjava.ch04partitions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Actor {
    private int id;
    private int idPartition;
    private String name;
    private int year;
}
