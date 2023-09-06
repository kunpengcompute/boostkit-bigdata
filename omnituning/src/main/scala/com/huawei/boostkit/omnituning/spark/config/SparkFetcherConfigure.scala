/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnituning.spark.config

import com.huawei.boostkit.omnituning.exception.OmniTuningException
import org.apache.commons.configuration2.PropertiesConfiguration

class SparkFetcherConfigure(propertiesConfiguration: PropertiesConfiguration) {
  val enableKey = "spark.enable"
  val eventLogModeKey = "spark.eventLogs.mode"
  val workloadKey = "spark.workload"
  val defaultWorkload = "default"
  val restEventLogMode = "rest"
  val restUrlKey = "spark.rest.url"
  val defaultRestUrl = "http://localhost:18080"
  val timeoutKey = "spark.timeout.seconds"
  val logEventLogMode = "log"
  val logDirectoryKey = "spark.log.directory"
  val maxLogFileSizeInMBKey = "spark.log.maxSize.mb"

  val defaultTimeoutSeconds = 30
  val defaultMaxLogSize = 500

  val enable: Boolean = propertiesConfiguration.getBoolean(enableKey, false)
  val mode: String = propertiesConfiguration.getString(eventLogModeKey)
  val restUrl: String = propertiesConfiguration.getString(restUrlKey, defaultRestUrl)
  val timeoutSeconds: Int = propertiesConfiguration.getInt(timeoutKey, defaultTimeoutSeconds)
  val logDirectory: String = propertiesConfiguration.getString(logDirectoryKey, "")
  val maxLogSizeInMB: Int = propertiesConfiguration.getInt(maxLogFileSizeInMBKey, defaultMaxLogSize)
  val workload: String = propertiesConfiguration.getString(workloadKey, defaultWorkload)

  def isRestMode: Boolean = {
    if (mode.equals(restEventLogMode)) {
      true
    } else if (mode.equals(logEventLogMode)) {
      false
    } else {
      throw new OmniTuningException(s"Unknown event log mode ${mode}")
    }
  }
}
