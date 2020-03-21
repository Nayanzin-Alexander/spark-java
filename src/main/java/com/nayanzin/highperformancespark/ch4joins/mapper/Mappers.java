package com.nayanzin.highperformancespark.ch4joins.mapper;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaCongratsDto;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import org.apache.commons.lang3.math.NumberUtils;
import scala.Tuple2;

import static java.lang.String.format;
import static java.util.Objects.isNull;
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

    // Csv schema: panda_id: Long, panda_address: String, panda_name: String
    public static Tuple2<Long, Tuple2<String, String>> csvLineToIdNameAddressOrNull(String line) {
        if (isNull(line)) {
            return null;
        }
        String[] cols = line.split(",");
        if (cols.length != 3) {
            return null;
        }

        Long id = toLong(cols[0], null);
        String name = cols[1];
        String address = cols[2];
        return new Tuple2<>(id, new Tuple2<>(name, address));
    }

    // Csv schema: panda_id: Long, score: Double
    public static Tuple2<Long, Double> csvLineToIdScore(String csvLine) {
        if (isNull(csvLine)) {
            return null;
        }
        String[] cols = csvLine.split(",");
        if (cols.length != 2) {
            return null;
        }
        Long id = toLong(cols[0], null);
        Double score = toDouble(cols[1], null);
        return new Tuple2<>(id, score);
    }

    // Scv schema id: Long, name: String, address: String, maxScore: Double
    public static String idMaxScoreNameAddressToCsvLine(Tuple2<Long, Tuple2<Double, Tuple2<String, String>>> idMaxScoreNameAddress) {
        Long id = idMaxScoreNameAddress._1;
        Double maxScore = idMaxScoreNameAddress._2._1;
        String name = idMaxScoreNameAddress._2._2._1;
        String address = idMaxScoreNameAddress._2._2._2;
        return format("%d,%s,%s,%f", id, name, address, maxScore);
    }

    private static Long toLong(String col, Long defaultValue) {
        NumberUtils.isParsable(col);
        try {
            return Long.parseLong(col);
        } catch (NumberFormatException nfe) {
            return defaultValue;
        }
    }

    private static Double toDouble(String col, Double defaultValue) {
        try {
            return Double.parseDouble(col);
        } catch (NumberFormatException nfe) {
            return defaultValue;
        }
    }
}
