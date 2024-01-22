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

  private def isPPDSafe(filters: Seq[Filter], dataSchema: TypeDescription): Seq[Boolean] = {
    def convertibleFiltersHelper(filter: Filter,
      dataSchema: TypeDescription): Boolean = filter match {
      case And(left, right) =>
        convertibleFiltersHelper(left, dataSchema) && convertibleFiltersHelper(right, dataSchema)
      case Or(left, right) =>
        convertibleFiltersHelper(left, dataSchema) && convertibleFiltersHelper(right, dataSchema)
      case Not(pred) =>
        convertibleFiltersHelper(pred, dataSchema)
      case other =>
        other match {
          case EqualTo(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case EqualNullSafe(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case LessThan(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case LessThanOrEqual(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case GreaterThan(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case GreaterThanOrEqual(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case IsNull(name) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case IsNotNull(name) =>
            dataSchema.findSubtype(name).getCategory != CHAR
          case In(name, _) =>
            dataSchema.findSubtype(name).getCategory != CHAR
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

      val fs = filePath.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val resultedColPruneInfo =
        Utils.tryWithResource(OrcFile.createReader(filePath, readerOptions)) { reader =>
          OrcUtils.requestedColumnIds(
            isCaseSensitive, dataSchema, requiredSchema, reader, conf)
        }

      val isPPDSafeValue =
        Utils.tryWithResource(OrcFile.createReader(filePath, readerOptions)) { reader =>
        isPPDSafe(filters, reader.getSchema).reduceOption(_ && _)
      }

      if (resultedColPruneInfo.isEmpty) {
        Iterator.empty
      } else {
        // ORC predicate pushdown
        if (orcFilterPushDown && isPPDSafeValue.getOrElse(false)) {
          OrcUtils.readCatalystSchema(filePath, conf, ignoreCorruptFiles).foreach {
            fileSchema => OrcFilters.createFilter(fileSchema, filters).foreach { f =>
              OrcInputFormat.setSearchArgument(conf, f, fileSchema.fieldNames)
            }
          }
        }

        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        val resultSchemaString = OrcUtils.orcResultSchemaString(canPruneCols,
          dataSchema, resultSchema, partitionSchema, conf)
        assert(requestedColIds.length == requiredSchema.length,
          "[BUG] requested column IDs do not match required schema")
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
          val requestedDataColIds = requestedColIds ++ Array.fill(partitionSchema.length)(-1)
          val requestedPartitionColIds =
            Array.fill(requiredSchema.length)(-1) ++ Range(0, partitionSchema.length)
        SparkMemoryUtils.init()
          batchReader.initialize(fileSplit, taskAttemptContext)
          batchReader.initBatch(
            requiredSchema.fields,
            resultSchema.fields,
            requestedDataColIds,
            requestedPartitionColIds,
            file.partitionValues)

          iter.asInstanceOf[Iterator[InternalRow]]
        }
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
