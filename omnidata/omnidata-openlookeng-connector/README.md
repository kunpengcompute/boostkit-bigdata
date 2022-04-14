# OmniData Connector

## Overview

OmniData Connector is a data source connector developed for openLooKeng. 

The OmniData connector allows querying data sources where c Server is deployed. It pushes down some operators such as filter to the OmniData service close to the storage to improve the performance of storage-computing-separated system.

## Building OmniData Connector

1. OmniData Connector is developed under the architecture of openLooKeng. You need to build openLooKeng first as a non-root user.
2. Simply run the following command from the project root directory:<br>
`mvn clean install -Dos.detected.arch="aarch64"`<br>
Then you will find omnidata-openlookeng-connector-*.zip under the omnidata-openlookeng-connector/connector/target/ directory.
OmniData Connector has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:<br>
`mvn clean install -DskipTests -Dos.detected.arch="aarch64"`<br>

## Deploying OmniData Connector

1. Unzip omnidata-openlookeng-connector-*.zip to the plugin directory of openLooKeng.
2. Obtain the latest OmniData software package, replace the boostkit-omnidata-client-\*.jar and boostkit-omnidata-core-\*.jar in the omnidata-openlookeng-connector-\* directory.
3. Set "connector.name=omnidata-openlookeng" in the openLooKeng catalog properties file.

## Contribution Guidelines

Track the bugs and feature requests via GitHub issues.

## More Information

For further assistance, send an email to kunpengcompute@huawei.com.

