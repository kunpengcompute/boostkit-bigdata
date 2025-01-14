/*
 * Copyright (C) 2021-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.util.ShimUtil

import java.net.URI

class OmniParquetFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  override def shortName(): String = "parquet-native"

  override def toString: String = "PARQUET-NATIVE"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[OmniParquetFileFormat]

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException()
  }

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val sqlConf = sparkSession.sessionState.conf

    val capacity = sqlConf.parquetVectorizedReaderBatchSize

    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown

    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val filePath = ShimUtil.getPartitionedFilePath(file)
      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          filePath,
          file.start,
          file.start + file.length,
          file.length,
          Array.empty,
          null)

      val sharedConf = broadcastedHadoopConf.value.value
      lazy val footerFileMetaData =
        ParquetFooterReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData

      val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        datetimeRebaseModeInRead)

      val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        int96RebaseModeInRead)

      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        filters.reduceOption(And(_, _))
      } else {
        None
      }

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      val nameToField = if (isCaseSensitive) {
        footerFileMetaData.getSchema.getFields.asScala.map(field => (field.getName, field)).toMap
      } else {
        CaseInsensitiveMap(footerFileMetaData.getSchema.getFields.asScala.map(field => (field.getName, field)).toMap)
      }
      val parquetTypes = requiredSchema.fieldNames.map(fieldName => nameToField.getOrElse(fieldName, null)).toList.asJava

      val batchReader = new OmniParquetColumnarBatchReader(capacity, requiredSchema, pushed.orNull,
        datetimeRebaseSpec, int96RebaseSpec, parquetTypes)

      val iter = new RecordReaderIterator(batchReader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
      SparkMemoryUtils.init()

      batchReader.initialize(split, hadoopAttemptContext)
      logDebug(s"Appending $partitionSchema ${file.partitionValues}")
      batchReader.initBatch(partitionSchema, file.partitionValues)

      // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
      iter.asInstanceOf[Iterator[InternalRow]]
    }
  }

}
