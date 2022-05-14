# omnidata-hive-connector



Introduction
============

The omnidata hive connector library running on Kunpeng processors is a Hive SQL plugin that pushes computing-side operators to storage nodes for computing. It is developed based on original APIs of Apache [Hive 3.1.0](https://github.com/apache/hive/tree/rel/release-3.1.0). This library applies to the big data storage separation scenario or large-scale fusion scenario where a large number of compute nodes read data from remote nodes. In this scenario, a large amount of raw data is transferred from storage nodes to compute nodes over the network for processing, resulting in a low proportion of valid data and a huge waste of network bandwidth. You can find the latest documentation, including a programming guide, on the project web page. This README file only contains basic setup instructions.


Building And Packageing
====================

(1) Build the project under the "omnidata-hive-connector" directory:

    mvn clean package

(2) Obtain the jar under the "omnidata-hive-connector/connector/target" directory.

Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues](https://github.com/kunpengcompute/omnidata-hive-connector/issues).

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.

