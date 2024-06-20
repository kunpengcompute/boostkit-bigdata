/*
 *
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

package com.huawei.analytics.shield.crypto.helper

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import java.io.{File => JFile}
import scala.collection.mutable.ArrayBuffer

abstract class ShieldSpecHelper extends FlatSpec with Matchers with BeforeAndAfter {
  private val tmpDirs: ArrayBuffer[JFile] = new ArrayBuffer[JFile]()

  def createTmpDir(name: String): JFile = {
    val directory = new JFile(name)
    directory.mkdir()
    tmpDirs.append(directory)
    directory
  }

  def doAfter(): Unit = {
  }

  def doBefore(): Unit = {
    if (SparkSession.getDefaultSession.isDefined) {
      SparkSession.getDefaultSession.get.stop()
    }
  }

  before {
    doBefore()
  }

  after {
    doAfter()
    tmpDirs.foreach(dir => {
      if (dir.exists()) {
        dir.deleteOnExit()
        dir.list().foreach { f =>
          val sub = new JFile(dir, f)
          if (sub.isFile) {
            sub.deleteOnExit()
          } else {
            FileUtils.deleteDirectory(sub)
          }
        }
      }
    })
  }

}
