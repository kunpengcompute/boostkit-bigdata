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

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig

import org.apache.spark.internal.Logging

trait RewriteLogger extends Logging {

  private def logLevel: String = OmniCachePluginConfig.getConf.logLevel

  private val logFlag = "[OmniCache]"

  def logBasedOnLevel(f: => String): Unit = {
    logLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f)
    }
  }

  override def logInfo(msg: => String): Unit = {
    super.logInfo(s"$logFlag $msg")
  }

  override def logDebug(msg: => String): Unit = {
    super.logDebug(s"$logFlag $msg")
  }

  override def logTrace(msg: => String): Unit = {
    super.logTrace(s"$logFlag $msg")
  }

  override def logWarning(msg: => String): Unit = {
    super.logWarning(s"$logFlag $msg")
  }

  override def logError(msg: => String): Unit = {
    super.logError(s"$logFlag $msg")
  }
}
