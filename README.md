# lzo2snappy

## Introduction

This is a spark (scala) project to generate hive tables (snappy / parquet) automatically from an hive table in lzo format.

The idea is use some strategies to solve this problem, RDD, Dataframe and Hive HQL. 
 
## Requirements

This project depends of lzo support enabled in your cluster, the instructions are found in this [Cloudera page](https://docs.cloudera.com/documentation/enterprise/6/6.2/topics/impala_txtfile.html#lzo). 

## Usage

### RDD strategy

l2s <lzo file location> <parquet file destination> <original_table_name> <delimiter> 

'''
$ spark-submit --class br.com.brainboss.lzordd.lzordd l2s.jar /user/hive/warehouse/hive_lzo /user/hive/warehouse/snappy_test hive_lzo ,
'''

### Dataframe strategy

l2s <lzo file location> <table destination>

'''
$ spark-submit --class br.com.brainboss.lzodf.lzodf l2s.jar /user/hive/warehouse/hive_lzo snappy_hive_lzo
''' 


