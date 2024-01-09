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

package com.huawei.boostkit.omniadvisor.spark.client

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.huawei.boostkit.omniadvisor.OmniAdvisorContext
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException
import com.huawei.boostkit.omniadvisor.models.AppResult
import com.huawei.boostkit.omniadvisor.spark.data.SparkRestAnalyticJob
import com.huawei.boostkit.omniadvisor.spark.utils.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkDataCollection
import org.apache.spark.status.api.v1.ApplicationInfo
import org.glassfish.jersey.client.ClientProperties
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, SimpleTimeZone}
import java.util.zip.ZipInputStream
import javax.ws.rs.client.{Client, ClientBuilder, WebTarget}
import javax.ws.rs.core.MediaType
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}
import scala.util.control.NonFatal

class SparkRestClient(historyUri: String, timeoutSeconds: Int, sparkConf: SparkConf, workload: String)
  extends SparkEventClient {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SparkRestClient])

  private val historyServerUri: URI = {
    val baseUri: URI = {
      if (historyUri.contains("http://")) {
        new URI(historyUri)
      } else {
        new URI(s"http://${historyUri}")
      }
    }
    require(baseUri.getPath == "")
    baseUri
  }

  val timeout: FiniteDuration = Duration(timeoutSeconds, SECONDS)
  private val API_V1_MOUNT_PATH = "api/v1"
  val IN_PROGRESS = ".inprogress"

  private val sparkRestObjectMapper: ObjectMapper with ScalaObjectMapper = {
    val dateFormat = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'")
      val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
      formatter.setCalendar(cal)
      formatter
    }

    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setDateFormat(dateFormat)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }

  private val client: Client = ClientBuilder.newClient()

  private var apiTarget: WebTarget = client.property(ClientProperties.CONNECT_TIMEOUT, timeout.toMillis.toInt)
    .property(ClientProperties.READ_TIMEOUT, timeout.toMillis.toInt)
    .target(historyServerUri)
    .path(API_V1_MOUNT_PATH)

  protected def setApiTarget(apiTarget: WebTarget): Unit = {
    this.apiTarget = apiTarget
  }

  override def fetchAnalyticJobs(startTimeMills: Long, finishedTimeMills: Long): List[AnalyticJob] = {
    val minDate = sparkRestObjectMapper.getDateFormat.format(new Date(startTimeMills))
    val maxDate = sparkRestObjectMapper.getDateFormat.format(new Date(finishedTimeMills))
    val appTarget = apiTarget.path("applications").queryParam("minDate", minDate).queryParam("maxDate", maxDate)

    try {
      LOG.info(s"calling REST API at ${appTarget.getUri}")
      val applications = getApplications(appTarget, sparkRestObjectMapper.readValue[Seq[ApplicationInfo]])
        .filter(job => OmniAdvisorContext.getInstance().getFinder.byId(job.id) == null)
      val analyticJobs = new ListBuffer[AnalyticJob]()
      for (appInfo <- applications) {
        val attempts = appInfo.attempts
        if (attempts.isEmpty) {
          LOG.info("application {} attempt is empty, skip it", appInfo.id)
        } else {
          analyticJobs += new SparkRestAnalyticJob(appInfo.id)
        }
      }
      analyticJobs.toList
    } catch {
      case NonFatal(e) =>
        LOG.error(s"error reading jobData ${appTarget.getUri}. Exception Message = ${e}")
        throw new OmniAdvisorException(e)
    }
  }

  override def fetchAnalyticResult(job: AnalyticJob): AppResult = {
    require(job.isInstanceOf[SparkRestAnalyticJob], "Require SparkRestAnalyticJob")
    val sparkJob = job.asInstanceOf[SparkRestAnalyticJob]
    val attemptTarget = getApplicationMetaData(sparkJob.getApplicationId)
    val logTarget = attemptTarget.path("logs")
    LOG.info(s"creating SparkApplication by calling REST API at ${logTarget.getUri} to get eventLogs")
    resource.managed {
      getApplicationLogs(logTarget)
    }.acquireAndGet { zipInputStream =>
      getLogInputStream(zipInputStream, logTarget) match {
        case (None, _) =>
          throw new OmniAdvisorException(s"Failed to read log for application ${sparkJob.getApplicationId}")
        case (Some(inputStream), fileName) =>
          val dataCollection = new SparkDataCollection()
          dataCollection.replayEventLogs(inputStream, fileName)
          dataCollection.getAppResult(workload)
      }
    }
  }

  private def getApplications[T](webTarget: WebTarget, converter: String => T): T = {
    converter(webTarget.request(MediaType.APPLICATION_JSON).get(classOf[String]))
  }

  private def getApplicationMetaData(appId: String): WebTarget = {
    val appTarget = apiTarget.path(s"applications/${appId}")
    LOG.info(s"calling REST API at ${appTarget.getUri}")

    val applicationInfo = getApplicationInfo(appTarget)

    val lastAttemptId = applicationInfo.attempts.maxBy {
      _.startTime
    }.attemptId
    lastAttemptId.map(appTarget.path).getOrElse(appTarget)
  }

  private def getApplicationInfo(appTarget: WebTarget): ApplicationInfo = {
    try {
      getApplications(appTarget, sparkRestObjectMapper.readValue[ApplicationInfo])
    } catch {
      case NonFatal(e) =>
        LOG.error(s"error reading applicationInfo ${appTarget.getUri}. Exception Message = ${e.getMessage}")
        throw e
    }
  }

  private def getApplicationLogs(logTarget: WebTarget): ZipInputStream = {
    try {
      val inputStream = logTarget.request(MediaType.APPLICATION_OCTET_STREAM)
        .get(classOf[java.io.InputStream])
      new ZipInputStream(new BufferedInputStream(inputStream))
    } catch {
      case NonFatal(e) =>
        LOG.error(s"error reading logs ${logTarget.getUri}. Exception Message = ${e.getMessage}")
        throw e
    }
  }

  private def getLogInputStream(zis: ZipInputStream, attemptTarget: WebTarget): (Option[InputStream], String) = {
    val entry = zis.getNextEntry
    if (entry == null) {
      LOG.warn(s"failed to resolve log for ${attemptTarget.getUri}")
      (None, "")
    } else {
      val entryName = entry.getName
      if (entryName.equals(IN_PROGRESS)) {
        throw new OmniAdvisorException(s"Application for the log ${entryName} has not finished yes.")
      }
      val codec = SparkUtils.compressionCodecForLogName(sparkConf, entryName)
      (Some(codec.map {
        _.compressedInputStream(zis)
      }.getOrElse(zis)), entryName)
    }
  }
}
