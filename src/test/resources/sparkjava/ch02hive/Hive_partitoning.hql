# https://www.youtube.com/watch?v=oaEOtQwdicg
# Connect               $HIVE_HOME/bin/beeline -n $USER -u jdbc:hive2://localhost:10000


Hive Partitioning
-----------------
    1) Create Hive table
        CREATE TABLE all_actors(
                id BIGINT,
                name STRING,
                year INT)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

    2) Load the data in Hive table
        LOAD DATA
            INPATH '/partitions/csv/'
            INTO TABLE all_actors;

    3) Create partition table
        CREATE TABLE actors(
                id BIGINT,
                name STRING)
            PARTITIONED BY (year INT);

    4) Load data into partition table from hive table
        INSERT OVERWRITE TABLE actors PARTITION(year)
            SELECT id, name, year FROM all_actors;


Hive Bucketing
--------------
    1) Create Bucket table
        CREATE TABLE actors_buck (
            id BIGINT,
            name STRING)
        PARTITIONED BY (year INT)
        CLUSTERED BY (id) SORTED BY (name) INTO 5 BUCKETS;

    2) Insert data into bucketed table.
        INSERT OVERWRITE TABLE actors_buck PARTITION(year)
            SELECT id, name, year FROM actors;