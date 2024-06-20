#!/bin/bash
# build file for OmniShield
# Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
set -ex
action=$1
if [ "$action" = 'compile' ]; then
  cd ../
  echo "do compile"
  mvn clean package -DskipTests=true
elif [ "$action" = 'ut' ]; then
  echo "do compile with ut test"
  codepath=$(pwd)/../
  cd ${codepath}/../
  hadooppath=$(pwd)
  tar -xvf hadoop-3.2.0.tar.gz
  ln -s hadoop-3.2.0 hadoop
  export HADOOP_HOME=${hadooppath}/hadoop
  export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}

  echo "edit kms config"
  cd ${HADOOP_HOME}/etc/hadoop
  # shellcheck disable=SC2016
  printf '<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hadoop.kms.http.port</name>
    <value>9600</value>
  </property>
  <property>
    <name>hadoop.kms.key.provider.uri</name>
    <value>jceks://file@/${user.home}/kms.keystore</value>
  </property>
    <property>
    <name>hadoop.security.keystore.java-keystore-provider.password-file</name>
    <value>kms.keystore.password</value>
  </property>
</configuration>' >kms-site.xml

  printf '<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hadoop.security.key.provider.path</name>
    <value>kms://http@localhost:9600/kms</value>
  </property>
</configuration>' >core-site.xml

  echo "gen keystore"
  keytool -genkey -alias 'build' -keystore ~/kms.jks -dname "CN=localhost, OU=localhost, O=localhost, L=SH, ST=SH, C=CN" -keypass 123456 -storepass 123456 -validity 180
  cd ${HADOOP_HOME}/etc/hadoop
  echo "123456" >kms.keystore.password

  echo "start kms"
  cd ${HADOOP_HOME}
  hadoop --daemon start kms

  echo "stat compile"
  cd ${codepath}
  mvn clean package
  mvn jacoco:report
else
    echo "Unknown action"
fi
