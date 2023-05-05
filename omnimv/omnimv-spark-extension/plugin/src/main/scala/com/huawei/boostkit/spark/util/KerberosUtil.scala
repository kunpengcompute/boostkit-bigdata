/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.util

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.sql.SparkSession

object KerberosUtil {

  /**
   * new configuration from spark
   */
  def newConfiguration(spark: SparkSession): Configuration = {
    val configuration: Configuration = spark.sessionState.newHadoopConf()
    newConfiguration(configuration)
  }

  /**
   * new configuration from configuration
   */
  def newConfiguration(configuration: Configuration): Configuration = {
    val xmls = Seq("hdfs-site.xml", "core-site.xml")
    val xmlDir = System.getProperty("omnimv.hdfs_conf", ".")
    xmls.foreach { xml =>
      val file = new File(xmlDir, xml)
      if (file.exists()) {
        configuration.addResource(new Path(file.getAbsolutePath))
      }
    }

    // security mode
    if ("kerberos".equalsIgnoreCase(configuration.get("hadoop.security.authentication"))) {
      val krb5Conf = System.getProperty("omnimv.krb5_conf", "/etc/krb5.conf")
      System.setProperty("java.security.krb5.conf", krb5Conf)
      val principal = System.getProperty("omnimv.principal")
      val keytab = System.getProperty("omnimv.keytab")
      if (principal == null || keytab == null) {
        throw new RuntimeException("omnimv.principal or omnimv.keytab cannot be null")
      }
      System.setProperty("java.security.krb5.conf", krb5Conf)
      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal, keytab)
    }
    configuration
  }
}
