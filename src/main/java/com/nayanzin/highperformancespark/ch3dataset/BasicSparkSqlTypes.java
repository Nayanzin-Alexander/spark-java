package com.nayanzin.highperformancespark.ch3dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static com.nayanzin.highperformancespark.utils.Utils.buildSparkSession;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.spark.sql.types.DataTypes.*;

/*
./gradlew clean build

INPUT=src/test/resources/highperformancespark/ch3dataset/sqlTypes.json
spark-submit    \
    --class com.nayanzin.highperformancespark.ch3dataset.BasicSparkSqlTypes    \
    build/libs/spark-java-1.0-SNAPSHOT.jar  \
    $INPUT
 */
public class BasicSparkSqlTypes {

    private static final StructType nestedStruct = createStructType(asList(
            createStructField("Byte1", ByteType, true),
            createStructField("Byte2", ByteType, true)));

    // 1-byte signed integers (-128, 127)
    public static final StructField BYTE_FIELD = createStructField("Byte", ByteType, true);

    // 2-byte sighed integers (-32,768, 32,767)
    public static final StructField SHORT_FIELD = createStructField("Short", ShortType, true);

    // 4-byte signed integers (–2,147,483,648, 2,147,483,647)
    public static final StructField INT_FIELD = createStructField("Int", IntegerType, true);

    // 8-byte signed integers (–9,223,372,036,854,775,808, 9,223,372,036,854,775,807)
    public static final StructField LONG_FIELD = createStructField("Long", LongType, true);

    // Arbitrary precision signed decimals
    public static final StructField BIG_DECIMAL_FIELD = createStructField("BigDecimal", DecimalType.apply(15, 5), true);

    // 4-byte floating-point number
    public static final StructField FLOAT_FIELD = createStructField("Float", FloatType, true);

    // 8-byte floating-point number
    public static final StructField DOUBLE_FIELD = createStructField("Double", DoubleType, true);

    /**
     * Array of bytes
     * Encode/decode to/from String
     * <code>
     * byte[] originalBytes = new byte[] { 1, 2, 3};
     * String base64Encoded = DatatypeConverter.printBase64Binary(originalBytes);
     * byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
     * </code>
     */
    public static final StructField BYTE_ARRAY_FIELD = createStructField("ByteArray", BinaryType, true);

    // true/false
    public static final StructField BOOLEAN_FIELD = createStructField("Boolean", BooleanType, true);

    // Date without time information. "yyyy-MM-dd". Internally is represented as the number of days from 1970-01-01
    public static final StructField DATE_FIELD = createStructField("Date", DateType, true);

    // Date with time information. (Second precision). Represents java.sql.Timestamp values WITH SECONDS PRECISION.
    public static final StructField TIMESTAMP_FIELD = createStructField("Timestamp", TimestampType, true);

    // Character string values (stored as UTF8)
    public static final StructField STRING_FIELD = createStructField("String", StringType, true);

    // Array of single type of element, containsNull true if any null elements.
    public static final StructField ARRAY_OF_CUSTOM_TYPES_FIELD = createStructField("ArrayOfCustomTypes", ArrayType.apply(nestedStruct, true), true);

    // Key/value map, valueContainsNull if any values are null
    public static final StructField MAP_OF_CUSTOM_TYPES_FIELD = createStructField("MapOfCustomTypes", MapType.apply(StringType, nestedStruct), true);

    // Named fields of possible heterogeneous types, similar to a case class or JavaBean.
    public static final StructField CASE_CLASS_FIELD = createStructField("CaseClass", nestedStruct, true);

    private static final StructType allSparkTypesSchema = createStructType(asList(
            BYTE_FIELD,
            SHORT_FIELD,
            INT_FIELD,
            LONG_FIELD,
            BIG_DECIMAL_FIELD,
            FLOAT_FIELD,
            DOUBLE_FIELD,
            BYTE_ARRAY_FIELD,
            BOOLEAN_FIELD,
            DATE_FIELD,
            TIMESTAMP_FIELD,
            STRING_FIELD,
            ARRAY_OF_CUSTOM_TYPES_FIELD,
            MAP_OF_CUSTOM_TYPES_FIELD,
            CASE_CLASS_FIELD
    ));

    public static void main(String[] args) {
        SparkSession spark = buildSparkSession("BasicSparkSqlTypes", emptyMap());
        Dataset<Row> json = spark
                .read()
                .schema(allSparkTypesSchema)
                .json(args[0]);
        json.printSchema();
        json.show(10, 300);
    }
}
