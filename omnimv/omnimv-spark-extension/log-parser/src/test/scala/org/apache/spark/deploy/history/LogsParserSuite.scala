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

import java.io.{FileInputStream, FileNotFoundException}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.time.DateUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class LogsParserSuite extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

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

  test("parse logs") {
    val path = this.getClass.getResource("/").getPath
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        .format(DateUtils.addDays(new Date(), 7))
    val args: Array[String] = Array(path, "eventlog/parse"
      , "log_parse_1646816941391", "2022-09-15 11:00", endTime)
    ParseLogs.main(args)
    val fis = new FileInputStream("eventlog/parse/log_parse_1646816941391.json")
    val lines = IOUtils.readLines(fis, "UTF-8")
    IOUtils.closeQuietly(fis)
    val json: Seq[Map[String, String]] = Json(DefaultFormats)
        .read[Seq[Map[String, String]]](lines.get(0))
    assert(json.exists(map =>
      map.contains("materialized views") &&
          map("physical plan").contains(map("materialized views"))
    ))
  }

  test("error_invalid_param") {
    assertThrows[RuntimeException] {
      val path = this.getClass.getResource("/").getPath
      val args: Array[String] = Array(path, "eventlog/parse")
      ParseLog.main(args)
    }
    assertThrows[RuntimeException] {
      val path = this.getClass.getResource("/").getPath
      val args: Array[String] = Array(path, "eventlog/parse"
        , "application_1663257594501_0003.lz4", "1")
      ParseLog.main(args)
    }
  }

  test("error_invalid_logname") {
    assertThrows[RuntimeException] {
      val path = this.getClass.getResource("/").getPath
      val args: Array[String] = Array(path, "eventlog/parse"
        , "xxx.lz4")
      ParseLog.main(args)
    }
  }

  test("error_log_not_exist") {
    assertThrows[FileNotFoundException] {
      val path = this.getClass.getResource("/").getPath
      val args: Array[String] = Array(path, "eventlog/parse"
        , "application_1663257594501_00031.lz4")
      ParseLog.main(args)
    }
  }
}
