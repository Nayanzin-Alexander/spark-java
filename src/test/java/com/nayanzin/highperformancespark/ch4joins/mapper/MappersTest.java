package com.nayanzin.highperformancespark.ch4joins.mapper;

import com.nayanzin.highperformancespark.ch4joins.dto.PandaAddress;
import com.nayanzin.highperformancespark.ch4joins.dto.PandaScore;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Java6Assertions.assertThat;


public class MappersTest {

    @Test
    public void validPandaAddressLineTest() {
        String csvLine = "1,Address 1,Andy";
        PandaAddress pandaAddress = Mappers.csvToPandaAddress(csvLine);
        assertThat(pandaAddress.getId()).isEqualTo(1L);
        assertThat(pandaAddress.getAddress()).isEqualTo("Address 1");
        assertThat(pandaAddress.getName()).isEqualTo("Andy");
        assertThat(pandaAddress.getErrors()).isNull();
    }

    @Test
    public void invalidNumberOfColumnsInPandaAddressLineTest() {
        String invalidNumberOfColumnsCsvLine = "33,1-,Address 1,Andy";
        PandaAddress pandaAddress = Mappers.csvToPandaAddress(invalidNumberOfColumnsCsvLine);
        assertThat(pandaAddress.getId()).isNull();
        assertThat(pandaAddress.getAddress()).isNull();
        assertThat(pandaAddress.getName()).isNull();
        assertThat(pandaAddress.getErrors()).isEqualTo(format("Wrong cols number in line: %s", "33,1-,Address 1,Andy"));
    }

    @Test
    public void invalidIdInPandaAddressLineTest() {
        String invalidIdCsvLine = "1-,Address 1,Andy";
        PandaAddress pandaAddress = Mappers.csvToPandaAddress(invalidIdCsvLine);
        assertThat(pandaAddress.getId()).isNull();
        assertThat(pandaAddress.getAddress()).isEqualTo("Address 1");
        assertThat(pandaAddress.getName()).isEqualTo("Andy");
        assertThat(pandaAddress.getErrors()).isEqualTo(format("Failed to parse id: %s", "1-"));
    }

    @Test
    public void validPandaScoreLineTest() {
        String validPandaScoreLine = "1,2.55";
        PandaScore score = Mappers.csvToPandaScore(validPandaScoreLine);
        assertThat(score.getId()).isEqualTo(1L);
        assertThat(score.getScore()).isEqualTo(2.55D);
        assertThat(score.getErrors()).isNull();
    }

    @Test
    public void invalidIdPandaScoreLineTest() {
        String validPandaScoreLine = "A1,2.55";
        PandaScore score = Mappers.csvToPandaScore(validPandaScoreLine);
        assertThat(score.getId()).isNull();
        assertThat(score.getScore()).isEqualTo(2.55D);
        assertThat(score.getErrors()).isEqualTo(format("Failed to parse id: %s", "A1"));
    }

    @Test
    public void invalidScorePandaScoreLineTest() {
        String validPandaScoreLine = "1,2.55A";
        PandaScore score = Mappers.csvToPandaScore(validPandaScoreLine);
        assertThat(score.getId()).isEqualTo(1L);
        assertThat(score.getScore()).isEqualTo(null);
        assertThat(score.getErrors()).isEqualTo(format("Failed to parse score: %s", "2.55A"));
    }

    @Test
    public void invalidNumberOfColumnsPandaScoreLineTest() {
        String validPandaScoreLine = "AA,1,2.55";
        PandaScore score = Mappers.csvToPandaScore(validPandaScoreLine);
        assertThat(score.getId()).isEqualTo(null);
        assertThat(score.getScore()).isEqualTo(null);
        assertThat(score.getErrors()).isEqualTo(format("Wrong cols number in line: %s", "AA,1,2.55"));
    }
}