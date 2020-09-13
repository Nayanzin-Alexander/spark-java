package com.nayanzin.usecases;

import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.RowFactory.create;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CountNonNullColumns extends JavaDataFrameSuiteBase implements Serializable {

    private static final StructType SCHEMA = createStructType(asList(
            createStructField("pivot", StringType, false),
            createStructField("A", StringType, false),
            createStructField("B", StringType, false),
            createStructField("C", StringType, false),
            createStructField("D", StringType, false),
            // ....
            createStructField("Z", StringType, false)));

    private Dataset<Row> dataset;

    @Before
    public void setUp() {
        dataset = spark().createDataFrame(asList(
                create("name1", "A", null, null, null, null),
                create("name1", null, "B", null, null, null),
                create("name1", null, "B", "C", null, null),
                create("name1", "A", null, "C", null, null),
                create("name2", null, "B", "C", null, null),
                create("name3", "A", null, null, "D", null),
                create("name4", null, null, null, "D", null)), SCHEMA);
    }

    @Test
    public void countNonNullColumns() {
        String pivotColumnName = "pivot";
        Report report = findMinimumOfPivotColumnValuesHavingMaximumNonNullColumns(dataset, pivotColumnName);

        assertThat(report.getNotFundNotNullColumns())
                .containsExactlyInAnyOrder("Z");

        assertThat(report.getPivotColumnValuesHavingMaxNumberOfNonNullColumns())
                .containsExactlyInAnyOrder("name1", "name3");
    }

    private Report findMinimumOfPivotColumnValuesHavingMaximumNonNullColumns(Dataset<Row> dataset, String pivotColumnName) {
        String numberOfNotNullColumnsName = "numberOfNotNullColumns";
        Dataset<Row> df = dataset;

        // Get column names except the pivot one
        List<String> columns = Arrays.stream(dataset.schema().fields())
                .map(StructField::name)
                .filter(name -> !name.equals(pivotColumnName))
                .collect(toList());

        //
        for (String name : columns) {
            df = df.withColumn(name, when(col(name).isNotNull(), 1).otherwise(0));
        }


        // Non null fields by col by name sum
        Column[] cols = new Column[columns.size() - 1];
        int i = -1;
        Column first = null;
        for (String name : columns) {
            if (i == -1) {
                i++;
                first = sum(name).as(name);
            } else {
                cols[i++] = sum(name).as(name);
            }
        }

        df = df.groupBy(pivotColumnName)
                .agg(first, cols);
        for (String name : columns) {
            df = df.withColumn(name, when(col(name).gt(0), 1).otherwise(0));
        }
        df.show();

        // Add `number of not null columns"
        Column sumOfAllColumnsExpression = col(columns.get(0));
        for (int j = 1; j < columns.size(); j++) {
            sumOfAllColumnsExpression = sumOfAllColumnsExpression.plus(col(columns.get(j)));
        }
        df = df.withColumn(numberOfNotNullColumnsName, sumOfAllColumnsExpression)
                .orderBy(col(numberOfNotNullColumnsName).desc());
        df.show();


        // Select minim number of names having in sum at least one value in each column
        Set<String> candidates = new HashSet<>();
        Set<String> notFundNotNullColumns = new HashSet<>(columns);
        for (String columnName : columns) {
            if (notFundNotNullColumns.contains(columnName)) {
                List<Row> names = df.filter(col(columnName).equalTo(1))
                        .orderBy(col(numberOfNotNullColumnsName).desc())
                        .takeAsList(1);
                if (!names.isEmpty()) {
                    Row row = names.get(0);
                    candidates.add(row.getAs(pivotColumnName));

                    // Populate alreadyFoundNotNullColumns to prevent double quering them
                    columns.forEach(colName -> {
                        if (row.getAs(colName).equals(1)) {
                            notFundNotNullColumns.remove(colName);
                        }
                    });
                }
            }
        }

        return new Report(candidates, notFundNotNullColumns);
    }
}

class Report {
    private final Set<String> pivotColumnValuesHavingMaxNumberOfNonNullColumns;
    private final Set<String> notFundNotNullColumns;

    public Report(Set<String> pivotColumnValuesHavingMaxNumberOfNonNullColumns, Set<String> notFundNotNullColumns) {
        this.pivotColumnValuesHavingMaxNumberOfNonNullColumns = unmodifiableSet(pivotColumnValuesHavingMaxNumberOfNonNullColumns);
        this.notFundNotNullColumns = unmodifiableSet(notFundNotNullColumns);
    }

    public Set<String> getPivotColumnValuesHavingMaxNumberOfNonNullColumns() {
        return pivotColumnValuesHavingMaxNumberOfNonNullColumns;
    }

    public Set<String> getNotFundNotNullColumns() {
        return notFundNotNullColumns;
    }
}
