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

package com.huawei.analytics.shield.utils

import com.huawei.analytics.shield.OmniContext
import com.huawei.analytics.shield.crypto.{AES_GCM_NOPADDING, AlgorithmMode, PLAIN_TEXT}
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

/**
 * Encrypt main class (as spark app).
 * This class is submitted as the main class of the Spark app for data encryption and decryption.
 */
object Encrypt {

  var arguments: EncryptArguments = new EncryptArguments()

  def main(args: Array[String]): Unit = {
    parse(args.toList, arguments)
    val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
    arguments.nPartition = getPartition(sparkSession)
    val sc: OmniContext = OmniContext.initOmniContext(sparkSession)
    arguments.operate match {
      case "encrypt" => {
        doEncrypt(sc)
      }
      case "decrypt" => {
        doDecrypt(sc)
      }
      case _ => invalidArgumentError("wrong operate, encrypt or decrypt are supported")
    }
  }

  private def doEncrypt(sc: OmniContext): Unit = {
    val plainTextReader = sc.getDataFrameReader(PLAIN_TEXT).addHeaderParam(arguments.withHeader)
    arguments.fileType match {
      case "csv" =>
        val df = plainTextReader.csv(arguments.inputPath).repartition(arguments.nPartition)
        val encryptWriter = sc.getDataFrameWriter(df, arguments.algorithm).addHeaderParam(arguments.withHeader)
        encryptWriter.csv(arguments.outputPath)
      case "json" =>
        val df = plainTextReader.json(arguments.inputPath)
        val encryptWriter = sc.getDataFrameWriter(df, arguments.algorithm)
        encryptWriter.json(arguments.outputPath)
      case "txt" =>
        val df = plainTextReader.text(arguments.inputPath)
        val encryptWriter = sc.getDataFrameWriter(df, arguments.algorithm)
        encryptWriter.text(arguments.outputPath)
      case _ =>
        invalidArgumentError("wrong file type, only CSV/JSON/TXT files are supported.")
    }
  }

  private def doDecrypt(sc: OmniContext): Unit = {
    val encryptDataReader = sc.getDataFrameReader(arguments.algorithm).addHeaderParam(arguments.withHeader)
    arguments.fileType match {
      case "csv" =>
        val df = encryptDataReader.csv(arguments.inputPath)
        val plainTextWriter = sc.getDataFrameWriter(df, PLAIN_TEXT).addHeaderParam(arguments.withHeader)
        plainTextWriter.csv(arguments.outputPath)
      case "json" =>
        val df = encryptDataReader.json(arguments.inputPath)
        val plainTextWriter = sc.getDataFrameWriter(df, PLAIN_TEXT)
        plainTextWriter.json(arguments.outputPath)
      case "txt" =>
        val df = encryptDataReader.text(arguments.inputPath)
        val plainTextWriter = sc.getDataFrameWriter(df, PLAIN_TEXT)
        plainTextWriter.text(arguments.outputPath)
      case _ =>
        invalidArgumentError("wrong file type, only CSV/JSON/TXT files are supported.")
    }
  }

  private def getPartition(sparkSession: SparkSession): Int = {
    if (arguments.nPartition == 0) {
      val hadoopConfig =
        if (sparkSession.sparkContext.hadoopConfiguration != null) {
          sparkSession.sparkContext.hadoopConfiguration
        } else {
          new Configuration()
        }
      val filePath = new Path(arguments.inputPath)
      val fs: FileSystem = FileSystem.get(new URI(filePath.toString), hadoopConfig)
      val fileStatus = fs.getFileStatus(filePath)
      val fileSize = fileStatus.getLen
      val blockSize = fs.getDefaultBlockSize(filePath)
      (fileSize / (blockSize + 1024) + 1).toInt
    } else {
      arguments.nPartition
    }
  }

  private def parse(args: List[String], encryptArguments: EncryptArguments): Unit = args match {
    case ("-i" | "--inputDataSourcePath") :: value :: tail =>
      encryptArguments.inputPath = value
      parse(tail, encryptArguments)

    case ("-o" | "--outputDataSinkPath") :: value :: tail =>
      encryptArguments.outputPath = value
      parse(tail, encryptArguments)

    case ("-a" | "--Algorithm") :: value :: tail =>
      encryptArguments.algorithm = AlgorithmMode.parse(value)
      parse(tail, encryptArguments)

    case ("-t" | "--filetype") :: value :: tail =>
      encryptArguments.fileType = value
      parse(tail, encryptArguments)

    case ("-e" | "--operate") :: value :: tail =>
      encryptArguments.operate = value
      parse(tail, encryptArguments)

    case ("-h" | "--header") :: value :: tail =>
      encryptArguments.withHeader = value
      parse(tail, encryptArguments)

    case ("-p" | "--partition") :: value :: tail =>
      try {
        val intValue = Some(value.toInt)
        encryptArguments.nPartition = intValue.get
        parse(tail, encryptArguments)
      } catch {
        case e: NumberFormatException => None
      }

    case Nil =>

    case _ =>
      val p = args.head
      System.err.println(s"Unknown param $p")
      printUsageAndExit()
  }

  private def printUsageAndExit(): Unit = {
    System.err.println(
      "please see the usage info\n" +
        "\n" +
        "Options:\n" +
        "  -i PATH,       --inputPath PATH        file input path. e.g. file://... or hdfs://...\n" +
        "  -o PATH,       --outputPath PATH       output path e.g. file://... or hdfs://...\n" +
        "  -a ALGORITHM,  --Algorithm ALGORITHM   algorithm mode, aes/gcm/nopadding \n" +
        "  -e OPERATE,    --operate OPERATE       encrypt or decrypt , default is encrypt\n" +
        "  -t FILETYPE,   --filetype FILETYPE     file type e.g. csv,json,txt\n")
    System.exit(1)
  }
}

class EncryptArguments(var inputPath: String = "input_data_path",
                       var outputPath: String = "output_save_path",
                       var algorithm: AlgorithmMode = AES_GCM_NOPADDING,
                       var fileType: String = "csv",
                       var operate: String = "encrypt",
                       var withHeader: String = "false",
                       var nPartition: Int = 0
                      )
