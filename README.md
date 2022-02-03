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
$ spark-submit --jars /opt/cloudera/parcels/GPLEXTRAS/jars/hadoop-lzo.jar --class br.com.brainboss.lzordd.lzordd l2s.jar /user/hive/warehouse/hive_lzo /user/hive/warehouse/hive_lzo_snappy hive_lzo ,
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
$ spark-submit --jars /opt/cloudera/parcels/GPLEXTRAS/jars/hadoop-lzo.jar --class br.com.brainboss.lzodf.lzodf l2s.jar /user/hive/warehouse/hive_lzo_snappy hive_lzo
``` 

### Configuration file

A default configuration file comes built with the jar, and has the following parameters and default values:

| Parameter        | Default value | Description                                                                                           |
|------------------|---------------|-------------------------------------------------------------------------------------------------------|
| master           | yarn          | Spark execution mode                                                                                  |
| kerberized       | false         | Wether the cluster is kerberized or not                                                               |
| principal        | null          | Kerberos principal. Required if 'kerberized' is 'true'                                                |
| catalog          | spark         | Which catalog should be used by Spark. From CDP 7 or above it is required to be "hive"                |
| metastore_uri    | null          | Hive metastore URI, required from CDP 7 or above                                                      |
| extraLibraryPath | null          | Extra libraries to be used by Spark. Required to set GLPEXTRAS hadoop-native libs from CDP 7 or above |

To change any of those values simply create a new file named "application.conf" in HOCON format and set any of the keys above. For the configuration file to be recognized by the driver it must be passed as an argument as following:

```
$ --conf "spark.driver.extraJavaOptions=-Dconfig.file=./application.conf"
```

The full dataframe execution example with configuration file would be:

```
$ spark-submit --conf "spark.driver.extraJavaOptions=-Dconfig.file=./application.conf" --class br.com.brainboss.lzodf.lzodf l2s.jar /user/hive/warehouse/hive_lzo_snappy hive_lzo
``` 