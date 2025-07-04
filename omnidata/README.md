## OmniData算子下推
#### 介绍
OmniData算子下推特性是一种将大数据引擎的算子下推到存储节点的服务，从而实现近数据计算，减少网络带宽占用，提升查询引擎的查询性能。该特性支持Spark组件下推Filter、Aggregation、Limit算子到存储节点CPU，支持对ORC/Parquet等主流数据类型的访问，实现近数据计算，减少无效数据在网络上的传输，提升大数据计算性能。

#### 操作系统
鲲鹏架构下的openEuler系（例如openEuler 22.03 LTS）操作系统以及CentOS 7.6 操作系统。

#### 使用说明
如何使用OmniData安装相关工具，请参见： https://www.hikunpeng.com/document/detail/zh/kunpengboostkithistory/2400/bds/kunpengbds_omniruntime_20_0103.html

#### 参与贡献
    如果您想为本仓库贡献代码，请向本仓库任意maintainer发送邮件
    如果您找到产品中的任何Bug，欢迎您提出ISSUE