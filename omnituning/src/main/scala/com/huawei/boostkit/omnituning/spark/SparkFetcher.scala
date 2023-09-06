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

package com.huawei.boostkit.omnituning.spark

import com.huawei.boostkit.omnituning.analysis.AnalyticJob
import com.huawei.boostkit.omnituning.fetcher.{Fetcher, FetcherType}
import com.huawei.boostkit.omnituning.models.AppResult
import com.huawei.boostkit.omnituning.spark.client.{SparkEventClient, SparkLogClient, SparkRestClient}
import com.huawei.boostkit.omnituning.spark.config.SparkFetcherConfigure
import com.huawei.boostkit.omnituning.spark.utils.SparkUtils
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Optional
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class SparkFetcher (configure: PropertiesConfiguration)
  extends Fetcher
{
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SparkFetcher])

  val sparkFetcherConfig = new SparkFetcherConfigure(configure)

  lazy val hadoopConfigure = new Configuration()

  lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    SparkUtils.getDefaultPropertiesFile() match {
      case Some(fileName) => sparkConf.setAll(SparkUtils.getPropertiesFromFile(fileName))
      case None => LOG.warn("Can't find Spark conf, use default config, Please set SPARK_HOME or SPARK_CONF_DIR")
    }
    sparkConf
  }

  lazy val sparkClient: SparkEventClient = {
    if (sparkFetcherConfig.isRestMode) {
      new SparkRestClient(sparkFetcherConfig.restUrl, sparkFetcherConfig.timeoutSeconds, sparkConf,
        sparkFetcherConfig.workload)
    } else {
      new SparkLogClient(hadoopConfigure, sparkConf, sparkFetcherConfig.logDirectory, sparkFetcherConfig.workload,
        sparkFetcherConfig.maxLogSizeInMB * FileUtils.ONE_MB)
    }
  }

  override def isEnable: Boolean = sparkFetcherConfig.enable

  override def analysis(job: AnalyticJob): Optional[AppResult] = {
    val appId = job.getApplicationId
    LOG.info(s"Fetching data for ${appId}")
    val result = Try {
      Await.result(doAnalysisApplication(job), Duration(sparkFetcherConfig.timeoutSeconds, SECONDS))
    }.transform(
      data => {
        LOG.info(s"Succeed fetching data for ${appId}")
        Success(data)
      },
      e => {
       LOG.error(s"Failed fetching data for ${appId}, Exception Message is ${e.getMessage}")
        Failure(e)
      })
    result match {
      case Success(data) => Optional.of(data)
      case Failure(e) => Optional.empty()
    }
  }

  private def doAnalysisApplication(job: AnalyticJob): Future[AppResult] = {
    Future {
      sparkClient.fetchAnalyticResult(job)
    }
  }

  override def getType: FetcherType = FetcherType.SPARK

  override def fetchAnalyticJobs(startTimeMills: Long, finishedTimeMills: Long): util.List[AnalyticJob] = {
    val jobs: util.List[AnalyticJob] = new util.ArrayList[AnalyticJob]()
    sparkClient.fetchAnalyticJobs(startTimeMills, finishedTimeMills).foreach(job => jobs.add(job))
    jobs
  }
}
