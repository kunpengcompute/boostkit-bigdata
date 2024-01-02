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

import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec, ZStdCompressionCodec}

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException, InputStream}
import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable
import scala.tools.jline_embedded.internal.InputStreamReader

object SparkUtils {
  def findApplicationFiles(hadoopConfiguration: Configuration, eventLogDir: String, startTimeMills: Long,
                           finishTimeMills: Long, maxFileSize: Long): List[String] = {
    val uri = new URI(eventLogDir)
    val fs = FileSystem.get(uri, hadoopConfiguration)
    val eventLogDirPath: Path = new Path(eventLogDir)
    if (fs.exists(eventLogDirPath) && fs.getFileStatus(eventLogDirPath).isDirectory) {
      fs.listStatus(eventLogDirPath).filter(status => {
        val fileSize = status.getLen
        val modifyTime = status.getModificationTime
        modifyTime >= startTimeMills && modifyTime <= finishTimeMills && fileSize <= maxFileSize
      }).map { status => status.getPath.toString }.toList
    } else {
      throw new OmniAdvisorException("eventLog path is not exist or not a Directory")
    }
  }

  private val IN_PROGRESS = ".inprogress"

  private val compressionCodecClassNamesByShortName = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName)

  private val compressionCodecMap = mutable.HashMap.empty[String, CompressionCodec]

  private def loadCompressionCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass = compressionCodecClassNamesByShortName.getOrElse(codecName.toLowerCase, codecName)
    val classLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val codec = try {
      val constructor = Class.forName(codecClass, true, classLoader).getConstructor(classOf[SparkConf])
      Some(constructor.newInstance(conf).asInstanceOf[CompressionCodec])
    } catch {
      case _: ClassNotFoundException => None
      case _: IllegalArgumentException => None
    }
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available."))
  }

  def compressionCodecForLogName(conf: SparkConf, logName: String): Option[CompressionCodec] = {
    val logBaseName = logName.stripSuffix(IN_PROGRESS)
    logBaseName.split("\\.").tail.lastOption.map {
      codecName =>
        compressionCodecMap.getOrElseUpdate(codecName, loadCompressionCodec(conf, codecName))
    }
  }

  def getApplicationIdFromFile(file: String): String = {
    val fileName = new Path(file).getName
    val logBaseName = fileName.stripSuffix(IN_PROGRESS)
    logBaseName.split("\\.").apply(0)
  }

  def withEventLog[T](fs: FileSystem, path: Path, codec: Option[CompressionCodec])(f: InputStream => T): T = {
    resource.managed { openEventLog(path, fs)}
      .map { in => codec.map { _.compressedInputStream(in) }.getOrElse(in) }
      .acquireAndGet(f)
  }

  private def openEventLog(logPath: Path, fs: FileSystem): InputStream = {
    if (!fs.exists(logPath)) {
      throw new FileNotFoundException(s"File ${logPath} does not exist.")
    }

    new BufferedInputStream(fs.open(logPath))
  }

  def defaultEnv: Map[String, String] = sys.env

  def getDefaultPropertiesFile(env: Map[String, String] = defaultEnv): Option[String] = {
    env.get("SPARK_CONF_DIR").orElse(env.get("SPARK_HOME").map {
      t => s"$t${File.separator}conf"})
      .map {t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  def getPropertiesFromFile(fileName: String): Map[String, String] = {
    val file = new File(fileName)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile, s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } finally {
      inReader.close()
    }
  }
}
