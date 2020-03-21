package com.nayanzin.highperformancespark.ch4joins.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Dto that contains all information to generate and send congratulation
 * mail to panda with it's highest score.
 *
 * Also contains errors field to collect errors for further processing and investigation.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PandaCongratsDto implements Serializable {
    private Long id;
    private String name;
    private String address;
    private Double highestScore;
    private String errors;
}
