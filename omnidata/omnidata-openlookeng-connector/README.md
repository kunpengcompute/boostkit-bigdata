# OmniData Connector

## 概述

OmniData Connector是为 openLooKeng 开发的数据源连接器。

OmniData Connector允许查询已部署 OmniData Server 的数据源。它将过滤（filter）等算子下推到靠近存储层的 OmniData 服务端执行，从而提升存算分离系统的性能。
## 构建 OmniData Connector

OmniData Connector基于 openLooKeng 架构开发。您首先需要以非 root 用户身份构建 openLooKeng。

在项目根目录下运行以下命令：

`mvn clean install -Dos.detected.arch="aarch64"`

构建完成后，您将在 omnidata-openlookeng-connector/connector/target/ 目录中找到 omnidata-openlookeng-connector-*.zip 文件。
OmniData Connector包含一套完整的单元测试，运行可能需要几分钟。您可以在构建时禁用测试：

`mvn clean install -DskipTests -Dos.detected.arch="aarch64"`

## 部署 OmniData Connector

1. 将 omnidata-openlookeng-connector-*.zip 解压到 openLooKeng 的 plugin 目录中。
2. 获取最新的 OmniData 软件包，并用其中的 boostkit-omnidata-server-\*.jar 文件替换解压后 omnidata-openlookeng-connector-\* 目录中的同名文件。
3. 在 openLooKeng 的 catalog 属性文件 中设置 connector.name=omnidata-openlookeng。

## 贡献指南

请通过 GitHub issues 跟踪问题（bugs）和功能需求（feature requests）。

## 更多信息

如需进一步帮助，请发送电子邮件至：kunpengcompute@huawei.com

