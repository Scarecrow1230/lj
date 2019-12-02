create external table if not exists hbase2hive2(
id int,
name string
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties(
"hbase.columns.mapping"=":key,f1:name"
)
tblproperties(
"hbase.table.name"="nsq:alj"
);


create external  table if not  exists hbase3hive(
id string,
name string
)
row format delimited fields terminated by ','
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties(
"hbase.columns.mapping"=":key,
f1:name"
)tblproperties(
"hbase.table.name"="nsq:alj"
)




create table mytable as select * from hbase2hive;


create table if not exists mytable3(
id string,
name string

)
row format delimited fields terminated by ',';

insert into mytable3 select * from  hbase3hive;







