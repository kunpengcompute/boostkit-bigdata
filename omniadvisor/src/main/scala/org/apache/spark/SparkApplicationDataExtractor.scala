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
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult
import com.huawei.boostkit.omniadvisor.spark.utils.ScalaUtils.{checkSuccess, parseMapToJsonString}
import com.huawei.boostkit.omniadvisor.spark.utils.SparkUtils
import com.nimbusds.jose.util.StandardCharset
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraph}
import org.apache.spark.status.api.v1._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object SparkApplicationDataExtractor {
  val LOG: Logger = LoggerFactory.getLogger(SparkApplicationDataExtractor.getClass)

  private val SPARK_REQUIRED_PARAMS_FILE = "SparkParams"

  private val SPARK_DEPLOY_MODE = "spark.submit.deployMode"
  private val SPARK_SUBMIT_CMD = "sun.java.command"
  private val SPARK_SQL_CLI_DRIVER = "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"

  def extractAppResultFromAppStatusStore(appInfo: ApplicationInfo,
                                         workload: String,
                                         environmentInfo: ApplicationEnvironmentInfo,
                                         jobsList: Seq[JobData],
                                         sqlExecutionsList: Seq[SQLExecutionUIData],
                                         sqlGraphMap: mutable.Map[Long, SparkPlanGraph]): ApplicationResult = {
    val appResult = new ApplicationResult()
    appResult.setApplicationId(appInfo.id)
    appResult.setApplicationName(appInfo.name)
    appResult.setJobType(FetcherType.SPARK.getName)
    appResult.setApplicationWorkload(workload)

    val configurations: Map[String, String] = extractAppConfigurations(environmentInfo)
    appResult.setParameters(parseMapToJsonString(extractRequiredConfiguration(configurations)))
    appResult.setDeploy_mode(configurations.getOrElse(SPARK_DEPLOY_MODE, ""))
    val (submitMethod, submitCmdFormatted) = extractSubmitInfo(configurations, appResult.getDeploy_mode)
    appResult.setSubmit_method(submitMethod)
    appResult.setSubmit_cmd(submitCmdFormatted)

    val attempt: ApplicationAttemptInfo = lastAttempt(appInfo)
    if (attempt.completed && jobsList.nonEmpty && checkSuccess(jobsList)) {
      saveSuccessfulStatus(appResult, jobsList, sqlExecutionsList, sqlGraphMap)
    } else {
      saveFailedStatus(appResult, attempt)
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
    val configurations = new mutable.HashMap[String, String]()
    configurations ++= environmentInfo.sparkProperties.toMap
    configurations ++= environmentInfo.systemProperties.toMap
    configurations.toMap
  }

  private def extractSubmitInfo(configurations: Map[String, String], deployMode: String): (String, String) = {
    val submitCmd = configurations.getOrElse(SPARK_SUBMIT_CMD, "")
    val submitMethod = submitCmd match {
      case cmd if cmd.contains(SPARK_SQL_CLI_DRIVER) => ApplicationResult.SPARK_SQL
      case cmd if cmd.nonEmpty => ApplicationResult.SPARK_SUBMIT
      case _ => ""
    }
    val submitCmdFormatted = SparkUtils.formatSubmitCmd(submitCmd, deployMode)
    (submitMethod, submitCmdFormatted)
  }

  private def saveFailedStatus(applicationResult: ApplicationResult, attempt: ApplicationAttemptInfo): Unit = {
    applicationResult.setExecutionStatus(ApplicationResult.FAILED_STATUS)
    applicationResult.setStartTime(attempt.startTime.getTime)
    applicationResult.setFinishTime(attempt.endTime.getTime)
    applicationResult.setDurationTime(ApplicationResult.FAILED_JOB_DURATION)
    applicationResult.setQuery("")
  }

  private def saveSuccessfulStatus(applicationResult: ApplicationResult, jobsList: Seq[JobData], sqlExecutionsList: Seq[SQLExecutionUIData], sqlGraphMap: mutable.Map[Long, SparkPlanGraph]): Unit = {
    applicationResult.setExecutionStatus(ApplicationResult.SUCCEEDED_STATUS)

    val (startTime, finishTime) = extractJobsTime(jobsList)
    applicationResult.setStartTime(startTime)
    applicationResult.setFinishTime(finishTime)
    applicationResult.setDurationTime(if (finishTime - startTime > 0)
      finishTime - startTime else ApplicationResult.FAILED_JOB_DURATION)

    if (applicationResult.getSubmit_method.equals(ApplicationResult.SPARK_SQL)) {
      applicationResult.setQuery(extractQuerySQL(sqlExecutionsList, sqlGraphMap))
    } else {
      applicationResult.setQuery("")
    }
  }

  private def extractJobsTime(jobsList: Seq[JobData]): (Long, Long) = {
    require(jobsList.nonEmpty)
    var startTime = Long.MaxValue
    var finishTime = 0L

    jobsList.foreach(job => {
      job.submissionTime.flatMap { submissionTime =>
        if (submissionTime.getTime < startTime) {
          Some(startTime = submissionTime.getTime)
        } else {
          None
        }
      }.foreach(_ => ())
      job.completionTime.flatMap { completionTime =>
        if (completionTime.getTime > finishTime) {
          Some(finishTime = completionTime.getTime)
        } else {
          None
        }
      }.foreach(_ => ())
    })

    (startTime, finishTime)
  }

  private def extractQuerySQL(sqlExecutionsList: Seq[SQLExecutionUIData], sqlGraphMap: mutable.Map[Long, SparkPlanGraph]): String = {
    require(sqlExecutionsList.nonEmpty)
    val descriptions: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]
    var previousExecutionDesc: Option[String] = None
    for ((execution, index) <- sqlExecutionsList.zipWithIndex) {
      if (index > 0) {
        val sqlGraph = sqlGraphMap(execution.executionId)
        if (execution.description.nonEmpty && !isDuplicatedQuery(execution, previousExecutionDesc, sqlGraph)) {
          descriptions += execution.description.trim
        }
      } else {
        if (execution.description.nonEmpty) {
          descriptions += execution.description.trim
        }
      }
      previousExecutionDesc = Some(execution.description)
    }
    descriptions.mkString(";\n")
  }

  private def lastAttempt(applicationInfo: ApplicationInfo): ApplicationAttemptInfo = {
    require(applicationInfo.attempts.nonEmpty)
    applicationInfo.attempts.last
  }

  private def isDuplicatedQuery(execution: SQLExecutionUIData, previousExecutionDesc: Option[String], sqlGraph: SparkPlanGraph): Boolean = {
    execution.description.equals(previousExecutionDesc.getOrElse("")) && sqlGraph.allNodes.size == 1 && sqlGraph.allNodes.head.name.equals("LocalTableScan")
  }
}
