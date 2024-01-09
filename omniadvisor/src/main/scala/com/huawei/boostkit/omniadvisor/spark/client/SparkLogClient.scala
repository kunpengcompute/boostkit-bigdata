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

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob
import com.huawei.boostkit.omniadvisor.models.AppResult
import com.huawei.boostkit.omniadvisor.spark.data.SparkLogAnalyticJob
import com.huawei.boostkit.omniadvisor.spark.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkApplicationDataExtractor.extractAppResultFromAppStatusStore
import org.apache.spark.SparkConf
import org.apache.spark.SparkDataCollection

class SparkLogClient(hadoopConfiguration: Configuration, sparkConf: SparkConf, eventLogUri: String,
                     workload: String, maxFileSize: Long)
  extends SparkEventClient {

  override def fetchAnalyticJobs(startTimeMills: Long, finishedTimeMills: Long): List[AnalyticJob] = {
    SparkUtils.findApplicationFiles(hadoopConfiguration, eventLogUri, startTimeMills, finishedTimeMills, maxFileSize)
      .map(file => new SparkLogAnalyticJob(SparkUtils.getApplicationIdFromFile(file), file))
      .filter(job => OmniAdvisorContext.getInstance().getFinder.byId(job.getApplicationId) == null)
  }

  override def fetchAnalyticResult(job: AnalyticJob): AppResult = {
    require(job.isInstanceOf[SparkLogAnalyticJob], "Require SparkLogAnalyticJob")
    val logJob = job.asInstanceOf[SparkLogAnalyticJob]
    val path = new Path(logJob.getFilePath)
    val compressCodec = SparkUtils.compressionCodecForLogName(sparkConf, path.getName)
    val dataCollection = new SparkDataCollection

    SparkUtils.withEventLog(
      FileSystem.get(path.toUri, hadoopConfiguration), path, compressCodec) { in =>
      dataCollection.replayEventLogs(in, path.toString)
    }

    dataCollection.getAppResult(workload)
  }
}
