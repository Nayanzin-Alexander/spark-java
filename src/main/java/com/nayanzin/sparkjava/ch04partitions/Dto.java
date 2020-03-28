package com.nayanzin.sparkjava.ch04partitions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Dto {
    private Long id;
    private int partition;
    private String text;
}
