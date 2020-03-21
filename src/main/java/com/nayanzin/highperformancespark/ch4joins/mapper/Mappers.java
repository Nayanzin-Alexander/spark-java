package com.nayanzin.highperformancespark.ch4joins.mapper;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;

import static java.lang.String.format;
import static java.util.Objects.nonNull;

public final class Mappers {

    // Csv schema: panda_id: Long, panda_address: String, panda_name: String
    public static PandaAddress csvToPandaAddress(String pandaAddressCsvLine) {
        String[] cols = pandaAddressCsvLine.split(",");
        PandaAddress pandaAddress = new PandaAddress();
        if (cols.length != 3) {
            String errorMessage = format("Wrong cols number in line: %s", pandaAddressCsvLine);
            pandaAddress.setErrors(errorMessage);
            return pandaAddress;
        }
        try {
            Long id = Long.parseLong(cols[0]);
            pandaAddress.setId(id);
        } catch (NumberFormatException nfe) {
            String errorMessage = format("Failed to parse id: %s", cols[0]);
            pandaAddress.setErrors(errorMessage);
        }
        pandaAddress.setAddress(cols[1]);
        pandaAddress.setName(cols[2]);
        return pandaAddress;
    }

    // Csv schema: panda_id: Long, score: Double
    public static PandaScore csvToPandaScore(String pandaScoreScvLine) {
        String[] cols = pandaScoreScvLine.split(",");
        PandaScore pandaScore = new PandaScore();
        if (cols.length != 2) {
            String errorMessage = format("Wrong cols number in line: %s", pandaScoreScvLine);
            pandaScore.setErrors(errorMessage);
            return pandaScore;
        }
        try {
            Long id = Long.parseLong(cols[0]);
            pandaScore.setId(id);
        } catch (NumberFormatException nfe) {
            String errorMessage = format("Failed to parse id: %s", cols[0]);
            pandaScore.setErrors(errorMessage);
        }
        try {
            Double id = Double.parseDouble(cols[1]);
            pandaScore.setScore(id);
        } catch (NumberFormatException nfe) {
            String errorMessage = format("Failed to parse score: %s", cols[1]);
            pandaScore.setErrors(errorMessage);
        }
        return pandaScore;
    }

    private Mappers() {}

    public static PandaCongratsDto addressAndScoreToPandaCongratsDto(PandaAddress pandaAddress, PandaScore pandaScore) {

        String errors = nonNull(pandaAddress.getErrors()) ? pandaAddress.getErrors() : "";
        errors += nonNull(pandaAddress.getErrors()) && nonNull(pandaScore.getErrors()) ? "\n" : "";
        errors += nonNull(pandaScore.getErrors()) ? pandaScore.getErrors() : "";
        errors = errors.length() > 0 ? errors : null;

        return new PandaCongratsDto(
                pandaAddress.getId(),
                pandaAddress.getName(),
                pandaAddress.getAddress(),
                pandaScore.getScore(),
                errors
        );
    }
}
