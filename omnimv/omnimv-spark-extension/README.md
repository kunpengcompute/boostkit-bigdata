# omnimv-spark-extension

A SQL Engine Extension for Spark SQL to support Materialized View

## Introduction

Experiments show that there are duplicate subqueries in batch query,and materialized view is a pre-computing and caching
technology to reduce repeated calculation in batch query in database field.
The AI algorithm is used to analyze historical Spark tasks and recommend the optimal materialized view under certain
conditions.
The Spark plugin is used to add materialized view management and execution plan rewriting capabilities, greatly
improving Spark computing efficiency.

## Environment for building OmniMV

```shell
# download
wget --no-check-certificate https://mirrors.huaweicloud.com/kunpeng/dist/Apache/hadoop-3.1.1.tar.gz
# unpack
tar -zxvf hadoop-3.1.1.tar.gz
# set environment variables
export HADOOP_HOME="${pwd}/haddoop-3.1.1"
```

## Build OmniMV

pull the OmniMV code and compile it to get the jar package

```shell
git clone https://gitee.com/kunpengcompute/boostkit-bigdata.git
cd boostkit-bigdata/omnimv/omnimv-spark-extension
# This step can be compiled, tested and packaged to get plugin/boostkit-omnimv-spark-${omniMV.version}.jar
mvn clean package
```
