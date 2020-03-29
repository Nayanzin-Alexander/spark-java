package com.nayanzin.sparkjava.ch03dataset;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static com.nayanzin.sparkjava.util.TestUtils.getTestResource;
import static java.math.BigDecimal.valueOf;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static org.apache.spark.sql.Encoders.INT;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DataSetTest extends JavaDatasetSuiteBase implements Serializable {

    private static final Path inputFile = getTestResource("/sparkjava/ch03dataset/input.csv");
    private static final StructType csvSchema = createStructType(asList(
            createStructField("name", StringType, true),
            createStructField("age", IntegerType, true),
            createStructField("state", StringType, true),
            createStructField("salary", DecimalType.apply(15, 5), true)));
    private static final Encoder<Person> asPersonEncoder = Encoders.bean(Person.class);
    private static final Encoder<AgeGroup> asAgeGroupEncoder = Encoders.bean(AgeGroup.class);
    private static final Encoder<PersonWithAgeGroupInfo> asPersonWithAgeGroupInfoEncoder = Encoders.bean(PersonWithAgeGroupInfo.class);

    @Test
    public void readFromCsvTest() {
        Dataset<Person> actualCsvDataset = spark()
                .read()
                .option("header", "true")
                .schema(csvSchema)
                .csv(inputFile.toString())
                .as(asPersonEncoder);
        assertThat(actualCsvDataset.collectAsList()).containsExactlyInAnyOrder(
                new Person("William Scott", 20, "WA", valueOf(445.022D)),
                new Person("John Doe", 34, "CA", valueOf(55.2D)),
                new Person("Antony Jones", 35, "CA", valueOf(44.22D)));
    }

    @Test
    public void filterTest() {
        Dataset<Person> dataset = spark().createDataset(asList(
                new Person("William Scott", 20, "WA", valueOf(445.022D)),
                new Person("John Doe", 34, "CA", valueOf(55.2D)),
                new Person("Antony Jones", 35, "CA", valueOf(44.22D))
        ), asPersonEncoder);
        Dataset<Person> personsWithNameStartsWithJ = dataset
                .filter((FilterFunction<Person>) person -> person.getName().startsWith("J"));
        assertThat(personsWithNameStartsWithJ.collectAsList()).containsExactly(
                new Person("John Doe", 34, "CA", valueOf(55.2D)));
    }

    @Test
    public void innerJoinTest() {
        Dataset<Person> persons = spark().createDataset(asList(
                new Person("William Scott", 20, "WA", valueOf(445.022D)),
                new Person("John Doe", 34, "CA", valueOf(55.2D)),
                new Person("Antony Jones", 35, "CA", valueOf(44.22D)),
                new Person("Bill Drill", 40, "OH", valueOf(78.22D))
        ), asPersonEncoder);

        Dataset<AgeGroup> ageGroups = spark().createDataset(asList(
                new AgeGroup(0, 20, "Age is between 0 and 20"),
                new AgeGroup(20, 30, "Age is between 20 and 30"),
                new AgeGroup(30, 40, "Age is between 30 and 40")),
                asAgeGroupEncoder);

        Dataset<PersonWithAgeGroupInfo> joined =
                persons.join(ageGroups, onAgeInRange(persons, ageGroups), "inner")
                        .withColumnRenamed("info", "ageGroupInfo")
                        .as(asPersonWithAgeGroupInfoEncoder);

        assertThat(joined.collectAsList()).containsExactlyInAnyOrder(
                new PersonWithAgeGroupInfo("William Scott", 20, "WA", valueOf(445.022D), "Age is between 20 and 30"),
                new PersonWithAgeGroupInfo("John Doe", 34, "CA", valueOf(55.2D), "Age is between 30 and 40"),
                new PersonWithAgeGroupInfo("Antony Jones", 35, "CA", valueOf(44.22D), "Age is between 30 and 40"));
    }

    private Column onAgeInRange(Dataset<Person> p, Dataset<AgeGroup> a) {
        return p.col("age").$greater$eq(a.col("ageLowerBound"))
                .and(p.col("age").$less(a.col("ageUpperBound")));
    }

    @Test
    public void joinWithBroadCastedDataTest() {
        Dataset<Person> persons = spark().createDataset(asList(
                new Person("William Scott", 20, "WA", valueOf(445.022D)),
                new Person("John Doe", 34, "CA", valueOf(55.2D)),
                new Person("Antony Jones", 35, "CA", valueOf(44.22D)),
                new Person("Bill Drill", 40, "OH", valueOf(78.22D))
        ), asPersonEncoder);

        TreeMap<Integer, AgeGroup> ageGroupsWithAgeLowerBoundAsKey = new TreeMap<>();
        ageGroupsWithAgeLowerBoundAsKey.put(0, new AgeGroup(0, 20, "Age is between 0 and 20"));
        ageGroupsWithAgeLowerBoundAsKey.put(20, new AgeGroup(20, 30, "Age is between 20 and 30"));
        ageGroupsWithAgeLowerBoundAsKey.put(30, new AgeGroup(30, 40, "Age is between 30 and 40"));


        Broadcast<TreeMap<Integer, AgeGroup>> ageGroupsWithAgeLowerBoundAsKeyBroadcast = jsc().broadcast(ageGroupsWithAgeLowerBoundAsKey);

        Dataset<PersonWithAgeGroupInfo> joinedWithBroadCasted = persons
                .map(joinOnAgeInRange(ageGroupsWithAgeLowerBoundAsKeyBroadcast), asPersonWithAgeGroupInfoEncoder);

        assertThat(joinedWithBroadCasted.collectAsList()).containsExactlyInAnyOrder(
                new PersonWithAgeGroupInfo("William Scott", 20, "WA", valueOf(445.022D), "Age is between 20 and 30"),
                new PersonWithAgeGroupInfo("John Doe", 34, "CA", valueOf(55.2D), "Age is between 30 and 40"),
                new PersonWithAgeGroupInfo("Antony Jones", 35, "CA", valueOf(44.22D), "Age is between 30 and 40"),
                new PersonWithAgeGroupInfo("Bill Drill", 40, "OH", valueOf(78.22D), null));
    }

    private MapFunction<Person, PersonWithAgeGroupInfo> joinOnAgeInRange(Broadcast<TreeMap<Integer, AgeGroup>> ageGroupsBroadcast) {
        TreeMap<Integer, AgeGroup> ageGroups = ageGroupsBroadcast.getValue();
        return (person) -> {
            String personsAgeGroupInfo = ofNullable(ageGroups.floorEntry(person.getAge()))
                    .map(Map.Entry::getValue)
                    .filter(ageGroup -> ageGroup.getAgeUpperBound() > person.getAge())
                    .map(AgeGroup::getInfo)
                    .orElse(null);
            return new PersonWithAgeGroupInfo(
                    person.getName(),
                    person.getAge(),
                    person.getState(),
                    person.getSalary(),
                    personsAgeGroupInfo);
        };
    }

    @Test
    public void unionTest() {
        Dataset<Integer> dataset1 = spark().createDataset(asList(1, 2, 3), INT());
        Dataset<Integer> dataset2 = spark().createDataset(asList(1, 2, 3, 4, 5), INT());
        Dataset<Integer> union = dataset1.union(dataset2);
        assertThat(union.collectAsList()).containsExactlyInAnyOrder(1, 1, 2, 2, 3, 3, 4, 5);
    }

    @Test
    public void intersectionTest() {
        Dataset<Integer> dataset1 = spark().createDataset(asList(0, 1, 2, 3, 4), INT());
        Dataset<Integer> dataset2 = spark().createDataset(asList(3, 4, 5, 6, 7), INT());
        Dataset<Integer> intersection = dataset1.intersect(dataset2);
        assertThat(intersection.collectAsList()).containsExactlyInAnyOrder(3, 4);
    }

    @Test
    public void distinctTest() {
        Dataset<Integer> dataset1 = spark().createDataset(asList(0, 0, 1, 2, 2), INT());
        Dataset<Integer> distinct = dataset1.distinct();
        assertThat(distinct.collectAsList()).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    public void outerJoinTest() {
        Dataset<Integer> dataset1 = spark().createDataset(asList(0, 0, 1, 2, 3, 4), INT());
        Dataset<Integer> dataset2 = spark().createDataset(asList(3, 4, 5, 6, 7, 7), INT());

        Dataset<Integer> intersection = dataset1.intersect(dataset2);
        Dataset<Integer> union = dataset1.union(dataset2);

        Dataset<Integer> outerJoin = union.except(intersection);
        assertThat(outerJoin.collectAsList()).containsExactlyInAnyOrder(0, 1, 2, 5, 6, 7);
    }
}
