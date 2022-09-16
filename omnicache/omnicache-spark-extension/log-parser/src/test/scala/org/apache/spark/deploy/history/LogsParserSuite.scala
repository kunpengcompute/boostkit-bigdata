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

package org.apache.spark.deploy.history

import java.io.FileInputStream

import org.apache.commons.io.IOUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import org.apache.spark.SparkFunSuite

class LogsParserSuite extends SparkFunSuite {

  test("parse") {
    val path = this.getClass.getResource("/").getPath
    val args: Array[String] = Array(path, "eventlog/parse"
      , "application_1663257594501_0003.lz4")
    ParseLog.main(args)
    val fis = new FileInputStream("eventlog/parse/application_1663257594501_0003.lz4.json")
    val lines = IOUtils.readLines(fis, "UTF-8")
    IOUtils.closeQuietly(fis)
    val json: Seq[Map[String, String]] = Json(DefaultFormats)
        .read[Seq[Map[String, String]]](lines.get(0))
    assert(json.exists(map =>
      map.contains("materialized views") &&
          map("physical plan").contains(map("materialized views"))
    ))
  }
}
