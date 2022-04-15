# omnidata-spark-connector



Introduction
============

The omnidata spark connector library running on Kunpeng processors is a Spark SQL plugin that pushes computing-side operators to storage nodes for computing. It is developed based on original APIs of Apache [Spark 3.0.0](https://github.com/apache/spark/tree/v3.0.0). This library applies to the big data storage separation scenario or large-scale fusion scenario where a large number of compute nodes read data from remote nodes. In this scenario, a large amount of raw data is transferred from storage nodes to compute nodes over the network for processing, resulting in a low proportion of valid data and a huge waste of network bandwidth. You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.


Building And Packageing
====================

(1) Build the project under the "omnidata-spark-connector" directory:

    mvn clean package

(2) Obtain the jar under the "omnidata-spark-connector/connector/target" directory.

Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues](https://github.com/kunpengcompute/omnidata-spark-connector/issues).

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.
