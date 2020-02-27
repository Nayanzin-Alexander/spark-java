package com.nayanzin.sparkjava.ch02etl;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import java.io.Serializable;

public class ETLTest extends SharedJavaSparkContext implements Serializable {

    // TODO implement unit tests for the following ECTL operations and Spark supported options.
    // ETL(ECTL) steps
    // 1. Extract (datasource, format, size)
    // 2. Cleansing (cleaning, validation, filtering, correction of incomplete data, deduplication)
    // 3. Transforming (data combining, aggregation and data encryption)
    // 4. Load (target system, number of generations, format, size)

    // Spark supports:
    // 1. Filesystems (LocalFS, HDFS, S3)
    // 2. File formats(text, XML, OBJ, CSV, JSON, Sequence)
    // 3. Sql data sources (PARQUET, JDBC, HIVE)
    // 4. NoSQL data sources (cassandra, HBase, mongoDB, Solr)
}
