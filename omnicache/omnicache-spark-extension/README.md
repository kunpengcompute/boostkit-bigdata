# omnicache-spark-extension

A SQL Engine Extension for Spark SQL to support Materialized View

## Introduction

Experiments show that there are duplicate subqueries in batch query,and materialized view is a pre-computing and caching
technology to reduce repeated calculation in batch query in database field.
The AI algorithm is used to analyze historical Spark tasks and recommend the optimal materialized view under certain
conditions.
The Spark plugin is used to add materialized view management and execution plan rewriting capabilities, greatly
improving Spark computing efficiency.

## compile environment

### windows

#### download hadoop

get hadoop-3.1.1.tar.gz and unpack

https://mirrors.huaweicloud.com/kunpeng/dist/Apache/hadoop-3.1.1.tar.gz

#### windows adaptor package

get hadoop-3.1.1/bin

https://gitee.com/helloxteen/winutils/tree/master

#### hadoop_bin overwrite

using windows adaptor package hadoop-3.1.1/bin overwrite hadoop-3.1.1/bin。

### set environment variables

HADOOP_HOME=unpack_dir/hadoop-3.1.1

Path=%HADOOP_HOME%/bin

### modify dir permission

On the disk where the compiled code is located, such as D disk.
Execute the following command to modify the permissions of the \tmp\hive directory, or create it manually if it does not
exist.
%HADOOP_HOME%\bin\winutils.exe chmod 777 D:\tmp\hive

### linux

```shell
# download
wget --no-check-certificate https://mirrors.huaweicloud.com/kunpeng/dist/Apache/hadoop-3.1.1.tar.gz
# unpack
tar -zxvf hadoop-3.1.1.tar.gz
# set environment variables
export HADOOP_HOME="${pwd}/haddoop-3.1.1"
```

## compile OmniCache code

pull the OmniCache code and compile it to get the jar package

```shell
git clone https://gitee.com/kunpengcompute/boostkit-bigdata.git
cd boostkit-bigdata/omnicache/omnicache-spark-extension
# This step can be compiled, tested and packaged to get plugin/boostkit-omnicache-spark-3.1.1-1.0.0.jar
mvn clean package
```
