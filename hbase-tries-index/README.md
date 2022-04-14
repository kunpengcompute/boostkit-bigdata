# hbase-trie-index



Introduction
============
The trie index library is used to accelerate HBase Data Block selection. By using Succinct Data Structure instead of ArrayList, trie index costs less memory and performs more efficiently.  The trie index library is based on the original APIs of Apache [HBase 2.2.3](https://github.com/apache/hbase/tree/rel/2.2.3).




Building And Packageing
====================

(1) Build the project:

    mvn clean package
    

Contribution Guidelines
========

Track the bugs and feature requests via GitHub [issues].

More Information
========

For further assistance, send an email to kunpengcompute@huawei.com.
