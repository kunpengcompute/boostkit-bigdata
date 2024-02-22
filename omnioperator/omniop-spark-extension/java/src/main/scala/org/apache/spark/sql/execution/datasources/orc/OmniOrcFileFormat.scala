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

package org.apache.spark.sql.execution.datasources.orc

import java.io.Serializable
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile, TypeDescription}
import org.apache.orc.TypeDescription.Category._
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.sql.types.StringType

import org.apache.spark.sql.types.DecimalType

class OmniOrcFileFormat extends FileFormat with DataSourceRegister with Serializable {

  override def shortName(): String = "orc-native"

  override def toString: String = "ORC-NATIVE"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OmniOrcFileFormat]

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    OrcUtils.inferSchema(sparkSession, files, options)
  }

  private def isPPDSafe(filters: Seq[Filter], dataSchema: StructType): Seq[Boolean] = {
    def convertibleFiltersHelper(filter: Filter,
      dataSchema: StructType): Boolean = filter match {
      case And(left, right) =>
        convertibleFiltersHelper(left, dataSchema) && convertibleFiltersHelper(right, dataSchema)
      case Or(left, right) =>
        convertibleFiltersHelper(left, dataSchema) && convertibleFiltersHelper(right, dataSchema)
      case Not(pred) =>
        convertibleFiltersHelper(pred, dataSchema)
      case other =>
        other match {
          case EqualTo(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case EqualNullSafe(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case LessThan(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case LessThanOrEqual(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case GreaterThan(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case GreaterThanOrEqual(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case IsNull(name) =>
            dataSchema.apply(name).dataType != StringType
          case IsNotNull(name) =>
            dataSchema.apply(name).dataType != StringType
          case In(name, _) =>
            dataSchema.apply(name).dataType != StringType
          case _ => false
        }
    }

    filters.map { filter =>
      convertibleFiltersHelper(filter, dataSchema)
    }
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val capacity = sqlConf.orcVectorizedReaderBatchSize

    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(hadoopConf, sqlConf.caseSensitiveAnalysis)

    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val orcFilterPushDown = sparkSession.sessionState.conf.orcFilterPushDown
    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      val filePath = new Path(new URI(file.filePath))
      val isPPDSafeValue = isPPDSafe(filters, dataSchema).reduceOption(_ && _)

      // ORC predicate pushdown
      if (orcFilterPushDown && isPPDSafeValue.getOrElse(false)) {
        OrcUtils.readCatalystSchema(filePath, conf, ignoreCorruptFiles).foreach {
          fileSchema => OrcFilters.createFilter(fileSchema, filters).foreach { f =>
            OrcInputFormat.setSearchArgument(conf, f, fileSchema.fieldNames)
          }
        }
      }

      val taskConf = new Configuration(conf)
      val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

      // read data from vectorized reader
      val batchReader = new OmniOrcColumnarBatchReader(capacity)
      // SPARK-23399 Register a task completion listener first to call `close()` in all cases.
      // There is a possibility that `initialize` and `initBatch` hit some errors (like OOM)
      // after opening a file.
      val iter = new RecordReaderIterator(batchReader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      // fill requestedDataColIds with -1, fil real values int initDataColIds function
      val requestedDataColIds = Array.fill(requiredSchema.length)(-1) ++ Array.fill(partitionSchema.length)(-1)
      val requestedPartitionColIds =
        Array.fill(requiredSchema.length)(-1) ++ Range(0, partitionSchema.length)

      // 初始化precision数组和scale数组，透传至java侧使用
      val requiredFields = requiredSchema.fields
      val fieldslength = requiredFields.length
      val precisionArray : Array[Int] = Array.ofDim[Int](fieldslength)
      val scaleArray : Array[Int] = Array.ofDim[Int](fieldslength)
      for ((reqField, index) <- requiredFields.zipWithIndex) {
        val reqdatatype = reqField.dataType
        if (reqdatatype.isInstanceOf[DecimalType]) {
          val precision = reqdatatype.asInstanceOf[DecimalType].precision
          val scale = reqdatatype.asInstanceOf[DecimalType].scale
          precisionArray(index) = precision
          scaleArray(index) = scale
        }
      }

      SparkMemoryUtils.init()
      batchReader.initialize(fileSplit, taskAttemptContext)
      batchReader.initDataColIds(requiredSchema, requestedPartitionColIds, requestedDataColIds, resultSchema.fields,
        precisionArray, scaleArray)
      batchReader.initBatch(
        requiredSchema.fields,
        resultSchema.fields,
        requestedDataColIds,
        requestedPartitionColIds,
        file.partitionValues)

      iter.asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException()
  }
}
