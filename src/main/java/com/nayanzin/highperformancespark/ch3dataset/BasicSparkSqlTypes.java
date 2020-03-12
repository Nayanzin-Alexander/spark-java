package com.nayanzin.highperformancespark.ch3dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.nayanzin.highperformancespark.Factory.buildSparkSession;
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
    private static final StructType allSparkTypesSchema = createStructType(asList(
            BYTE_FIELD,
            SHORT_FIELD,
            INT_FIELD,
            LONG_FIELD,
            BIG_DECIMAL_FIELD,
            FLOAT_FIELD,
            DOUBLE_FIELD,
            BYTE_ARRAY_FIELD
//            createStructField("Boolean", BooleanType, true),
//            createStructField("Date", DateType, true),
//            createStructField("Timestamp", TimestampType, true),
//            createStructField("String", StringType, true),
//            createStructField("ArrayOfCustomTypes", ArrayType.apply(customType, true), true),
//            createStructField("MapOfCustomTypes", MapType.apply(LongType, customType), true),
//            createStructField("CaseClass", customType, true)
    ));

    public static void main(String[] args) {
        SparkSession spark = buildSparkSession("BasicSparkSqlTypes", emptyMap());
        Dataset<Row> json = spark
                .read()
                .schema(allSparkTypesSchema)
                .json(args[0]);
        json.printSchema();
        json.show();
    }
}
