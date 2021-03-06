package com.nayanzin.highperformancespark.ch4joins.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatasetPandaAddress implements Serializable {
    private Long id;
    private String address;
    private String name;
    private Integer partition;
}
