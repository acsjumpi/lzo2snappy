# lzo2snappy

## Introduction

This is a spark (scala) project to generate hive tables (snappy / parquet) automatically from an hive table in lzo format.

The idea is use some strategies to solve this problem, RDD, Dataframe and Hive HQL. 
 
## Requirements

This project depends of lzo support enabled in your cluster, the instructions are found in this [Cloudera page](https://docs.cloudera.com/documentation/enterprise/6/6.2/topics/impala_txtfile.html#lzo). 

## Usage

### RDD strategy

For use this class, you need to follow the instructions below:

```
l2s <lzo file location> <parquet/snappy file destination> <original table name> [delimiter] 
```

Where: 

- `<lzo file location>` : Where lzo files are located
- `<parquet/snappy file destination>` : What is the location are new files need to be placed
- `<original table name>` : The name of the original table
- `[delimiter]` : The delimiter used to create the original table. Optional, defaults to ','

RDD execution example:

```
$ spark-submit --class br.com.brainboss.lzordd.lzordd l2s.jar /user/hive/warehouse/hive_lzo /user/hive/warehouse/hive_lzo_snappy hive_lzo ,
```

### Dataframe strategy

For use this class, you need to follow the instructions below:

```
l2s <parquet/snappy file destination> <original table name>
```

Where: 

- `<parquet/snappy file destination>` : What is the location are new files need to be placed
- `<original table name>` : The name of the original table

Dataframe execution example: 

```
$ spark-submit --class br.com.brainboss.lzodf.lzodf l2s.jar /user/hive/warehouse/hive_lzo_snappy hive_lzo
``` 
