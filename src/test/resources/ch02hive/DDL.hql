# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
# Connect               $HIVE_HOME/bin/beeline -n $USER -u jdbc:hive2://localhost:10000

# Metadata
HOW DATABASES;
CREATE  DATABASE    db_name;
DROP    DATABASE    db_name;
USE     db_name;
SHOW TABLES;

CREATE TABLE scores (
        id      INT     COMMENT 'User id',
        score   DOUBLE  COMMENT 'Score value'
    )
    row format delimited fields terminated by ','
    tblproperties  (
        "skip.header.line.count"="1"
    );

DESCRIBE scores;

LOAD DATA
    INPATH '/scores_300mb.csv'
    INTO TABLE scores;



