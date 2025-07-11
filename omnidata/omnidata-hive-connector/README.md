# omnidata-hive-connector



## 简介

基于鲲鹏处理器运行的 omnidata hive connector 库是一个 Hive SQL 插件，其功能是将计算端的算子下推到存储节点进行计算。该库基于 Apache Hive 3.1.0 的原始 API 开发。
此库适用于大数据存算分离场景或大规模存算融合场景，即大量计算节点从远端节点读取数据。在此类场景中，海量原始数据通过网络从存储节点传输到计算节点进行处理，导致有效数据比例低，造成巨大的网络带宽浪费。
您可以在项目网页上找到最新文档（包括编程指南）。本 README 文件仅包含基本的设置说明。

## 构建与打包

(1) 在 omnidata-hive-connector 项目目录下执行构建命令：

`mvn clean package`

(2) 在 omnidata-hive-connector/connector/target 目录下获取生成的 jar 文件。

## 贡献指南

请通过 GitHub issues 跟踪问题（bugs）和功能需求（feature requests）。

## 更多信息

如需进一步帮助，请发送电子邮件至：kunpengcompute@huawei.com

