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

package com.huawei.boostkit.omniadvisor.spark.utils

import com.alibaba.fastjson.JSONObject
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1.JobData

object ScalaUtils {
  def parseMapToJsonString(map: Map[String, String]): String = {
    val json = new JSONObject
    map.foreach(m => {
      json.put(m._1, m._2)
    })
    json.toJSONString
  }

  def checkSuccess(jobs: Seq[JobData]): Boolean = {
    !jobs.exists(_.status.equals(JobExecutionStatus.FAILED))
  }
}
