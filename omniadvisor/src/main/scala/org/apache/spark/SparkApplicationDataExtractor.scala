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

package org.apache.spark

import com.huawei.boostkit.omniadvisor.fetcher.FetcherType
import com.huawei.boostkit.omniadvisor.models.AppResult
import com.huawei.boostkit.omniadvisor.spark.utils.ScalaUtils.{checkSuccess, parseMapToJsonString}
import com.nimbusds.jose.util.StandardCharset
import org.apache.spark.status.api.v1._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object SparkApplicationDataExtractor {
  val LOG: Logger = LoggerFactory.getLogger(SparkApplicationDataExtractor.getClass)

  val SPARK_REQUIRED_PARAMS_FILE = "SparkParams"

  def extractAppResultFromAppStatusStore(appInfo: ApplicationInfo,
                                         workload: String,
                                         environmentInfo: ApplicationEnvironmentInfo,
                                         jobsList: Seq[JobData]): AppResult = {
    val appResult = new AppResult
    appResult.applicationId = appInfo.id
    appResult.applicationName = appInfo.name
    appResult.jobType = FetcherType.SPARK.getName
    appResult.applicationWorkload = workload

    val attempt: ApplicationAttemptInfo = lastAttempt(appInfo)
    appResult.startTime = attempt.startTime.getTime
    appResult.finishTime = attempt.endTime.getTime

    val configurations: Map[String, String] = extractAppConfigurations(environmentInfo)
    appResult.parameters = parseMapToJsonString(extractRequiredConfiguration(configurations))

    if (!attempt.completed) {
      // In this case, the task is killed, consider as a failed task
      appResult.executionStatus = AppResult.FAILED_STATUS
      appResult.durationTime = AppResult.FAILED_JOB_DURATION
      appResult.query = ""
    } else {
      if (jobsList.nonEmpty) {
        val query: Option[String] = jobsList.maxBy(job => job.jobId).description

        if (checkSuccess(jobsList)) {
          appResult.executionStatus = AppResult.SUCCEEDED_STATUS
          appResult.durationTime = attempt.duration
          appResult.query = query.getOrElse("")
        } else {
          appResult.executionStatus = AppResult.FAILED_STATUS
          appResult.durationTime = AppResult.FAILED_JOB_DURATION
          appResult.query = ""
        }
      } else {
        appResult.query = ""
        appResult.executionStatus = AppResult.FAILED_STATUS
        appResult.durationTime = AppResult.FAILED_JOB_DURATION
      }
    }

    appResult
  }

  private def extractRequiredConfiguration(sparkConfigure: Map[String, String]): Map[String, String] = {
    var sparkParamsFile: BufferedSource = null
    try {
      sparkParamsFile = Source.fromFile(Thread.currentThread().getContextClassLoader
        .getResource(SPARK_REQUIRED_PARAMS_FILE).getPath, StandardCharset.UTF_8.name)
      val params: Iterator[String] = sparkParamsFile.getLines()
      val requiredParams = new mutable.HashMap[String, String]()
      for (param <- params) {
        val paramRequired = param.trim
        if (paramRequired.nonEmpty) {
          requiredParams.put(paramRequired, sparkConfigure.getOrElse(paramRequired, ""))
        }
      }
      requiredParams.toMap[String, String]
    } finally {
      if (sparkParamsFile.nonEmpty) {
        sparkParamsFile.close
      }
    }
  }

  private def extractAppConfigurations(environmentInfo: ApplicationEnvironmentInfo): Map[String, String] = {
    environmentInfo.sparkProperties.toMap
  }

  def lastAttempt(applicationInfo: ApplicationInfo): ApplicationAttemptInfo = {
    require(applicationInfo.attempts.nonEmpty)
    applicationInfo.attempts.last
  }
}
