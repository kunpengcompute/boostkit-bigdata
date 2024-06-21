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
                                         sqlGraphMap: mutable.Map[Long, SparkPlanGraph]): AppResult = {
    val appResult = new AppResult
    appResult.applicationId = appInfo.id
    appResult.applicationName = appInfo.name
    appResult.jobType = FetcherType.SPARK.getName
    appResult.applicationWorkload = workload

    val configurations: Map[String, String] = extractAppConfigurations(environmentInfo)
    appResult.parameters = parseMapToJsonString(extractRequiredConfiguration(configurations))
    appResult.deploy_mode = configurations.getOrElse(SPARK_DEPLOY_MODE, "")
    val (submitMethod, submitCmdFormatted) = extractSubmitInfo(configurations, appResult.deploy_mode)
    appResult.submit_method = submitMethod
    appResult.submit_cmd = submitCmdFormatted

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
      case cmd if cmd.contains(SPARK_SQL_CLI_DRIVER) => AppResult.SPARK_SQL
      case cmd if cmd.nonEmpty => AppResult.SPARK_SUBMIT
      case _ => ""
    }
    val submitCmdFormatted = SparkUtils.formatSubmitCmd(submitCmd, deployMode)
    (submitMethod, submitCmdFormatted)
  }

  private def saveFailedStatus(appResult: AppResult, attempt: ApplicationAttemptInfo): Unit = {
    appResult.executionStatus = AppResult.FAILED_STATUS
    appResult.startTime = attempt.startTime.getTime
    appResult.finishTime = attempt.endTime.getTime
    appResult.durationTime = AppResult.FAILED_JOB_DURATION
    appResult.query = ""
  }

  private def saveSuccessfulStatus(appResult: AppResult, jobsList: Seq[JobData], sqlExecutionsList: Seq[SQLExecutionUIData], sqlGraphMap: mutable.Map[Long, SparkPlanGraph]): Unit = {
    appResult.executionStatus = AppResult.SUCCEEDED_STATUS

    val (startTime, finishTime) = extractJobsTime(jobsList)
    appResult.startTime = startTime
    appResult.finishTime = finishTime
    appResult.durationTime = if (finishTime - startTime > 0)
      finishTime - startTime else AppResult.FAILED_JOB_DURATION

    if (appResult.submit_method.equals(AppResult.SPARK_SQL)) {
      appResult.query = extractQuerySQL(sqlExecutionsList, sqlGraphMap)
    } else {
      appResult.query = ""
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
          descriptions += removeComment(execution.description).trim
        }
      } else {
        if (execution.description.nonEmpty) {
          descriptions += removeComment(execution.description).trim
        }
      }
      previousExecutionDesc = Some(execution.description)
    }
    val queryStr = descriptions.mkString(";\n")
    queryStr + (if (queryStr.endsWith(";")) "" else ";")
  }

  private def removeComment(query: String): String = {
    val queryLines = query.split("\n")
    val processedQuery = queryLines.map { queryLine =>
      if (queryLine.startsWith("--")) ""
      else if (queryLine.contains("--")) queryLine.split("--").head
      else queryLine
    }.filterNot(_ == "")
    processedQuery.mkString("\n")
  }

  private def lastAttempt(applicationInfo: ApplicationInfo): ApplicationAttemptInfo = {
    require(applicationInfo.attempts.nonEmpty)
    applicationInfo.attempts.last
  }

  private def isDuplicatedQuery(execution: SQLExecutionUIData, previousExecutionDesc: Option[String], sqlGraph: SparkPlanGraph): Boolean = {
    execution.description.equals(previousExecutionDesc.getOrElse("")) && sqlGraph.allNodes.size == 1 &&
      (sqlGraph.allNodes.head.name.equals("LocalTableScan") || sqlGraph.allNodes.head.name.equals("CommandResult"))
  }
}
