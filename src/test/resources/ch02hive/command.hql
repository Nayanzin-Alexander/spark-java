show databases;
create database <db_name>;
drop database <db_name>;
use <db_name>;

create table table_name
(Id INT, Name STRING, Dept STRIGN, Yoj INT, salary INT)
row format delimited fieldterminated by ','
tblproperties ("skip.header.line.count"="1");

show tables;
describe table_name;

select * from table_name;


