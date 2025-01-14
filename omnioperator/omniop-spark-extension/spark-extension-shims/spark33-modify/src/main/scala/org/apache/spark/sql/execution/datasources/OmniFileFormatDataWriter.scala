/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Cast, Concat, Expression, Literal, ScalaUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.orc.OmniOrcOutputWriter
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import scala.collection.mutable


/** Writes data to a single directory (used for non-dynamic-partition writes). */
class OmniSingleDirectoryDataWriter(
                                     description: WriteJobDescription,
                                     taskAttemptContext: TaskAttemptContext,
                                     committer: FileCommitProtocol,
                                     customMetrics: Map[String, SQLMetric] = Map.empty)
  extends FileFormatDataWriter(description, taskAttemptContext, committer, customMetrics) {
  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  // Initialize currentWriter and statsTrackers
  newOutputWriter()

  private def newOutputWriter(): Unit = {
    recordsInFile = 0
    releaseResources()

    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = committer.newTaskTempFile(
      taskAttemptContext,
      None,
      f"-c$fileCounter%03d" + ext)

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    currentWriter match {
      case _: OmniOrcOutputWriter =>
        currentWriter.asInstanceOf[OmniOrcOutputWriter]
          .initialize(description.allColumns, description.dataColumns)
      case _ =>
        throw new UnsupportedOperationException
        (s"Unsupported ${currentWriter.getClass} Output writer!")
    }
    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(record: InternalRow): Unit = {
    assert(record.isInstanceOf[OmniInternalRow])
    if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
      fileCounter += 1
      assert(fileCounter < MAX_FILE_COUNTER,
        s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

      newOutputWriter()
    }

    currentWriter.write(record)
    statsTrackers.foreach(_.newRow(currentWriter.path, record))
    recordsInFile += record.asInstanceOf[OmniInternalRow].batch.numRows()
  }
}

/**
 * Holds common logic for writing data with dynamic partition writes, meaning it can write to
 * multiple directories (partitions) or files (bucketing).
 */
abstract class OmniBaseDynamicPartitionDataWriter(
                                                   description: WriteJobDescription,
                                                   taskAttemptContext: TaskAttemptContext,
                                                   committer: FileCommitProtocol,
                                                   customMetrics: Map[String, SQLMetric])
  extends FileFormatDataWriter(description, taskAttemptContext, committer, customMetrics) {

  /** Flag saying whether or not the data to be written out is partitioned. */
  protected val isPartitioned = description.partitionColumns.nonEmpty

  /** Flag saying whether or not the data to be written out is bucketed. */
  protected val isBucketed = description.bucketSpec.isDefined

  assert(isPartitioned || isBucketed,
    s"""DynamicPartitionWriteTask should be used for writing out data that's either
       |partitioned or bucketed. In this case neither is true.
       |WriteJobDescription: $description
       """.stripMargin)

  /** Number of records in current file. */
  protected var recordsInFile: Long = _

  /**
   * File counter for writing current partition or bucket. For same partition or bucket,
   * we may have more than one file, due to number of records limit per file.
   */
  protected var fileCounter: Int = _

  /** Extracts the partition values out of an input row. */
  protected lazy val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(description.partitionColumns, description.allColumns)
    row => proj(row)
  }

  /** Expression that given partition columns builds a path string like: col1=val/col2=val/... */
  private lazy val partitionPathExpression: Expression = Concat(
    description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  /**
   * Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
   * the partition string.
   */
  private lazy val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression), description.partitionColumns)
    row => proj(row).getString(0)
  }

  /** Given an input row, returns the corresponding `bucketId` */
  protected lazy val getBucketId: InternalRow => Int = {
    val proj =
      UnsafeProjection.create(Seq(description.bucketSpec.get.bucketIdExpression),
        description.allColumns)
    row => proj(row).getInt(0)
  }

  /** Returns the data columns to be written given an input row */
  protected val getOutputRow =
    UnsafeProjection.create(description.dataColumns, description.allColumns)

  protected def getPartitionPath(partitionValues: Option[InternalRow],
                                 bucketId: Option[Int]): String = {
    val partDir = partitionValues.map(getPartitionPath(_))
    partDir.foreach(updatedPartitions.add)

    val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

    // The prefix and suffix must be in a form that matches our bucketing format. See BucketingUtils
    // for details. The prefix is required to represent bucket id when writing Hive-compatible
    // bucketed table.
    val prefix = bucketId match {
      case Some(id) => description.bucketSpec.get.bucketFileNamePrefix(id)
      case _ => ""
    }
    val suffix = f"$bucketIdStr.c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val fileNameSpec = FileNameSpec(prefix, suffix)

    val customPath = partDir.flatMap { dir =>
      description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
    }
    val currentPath = if (customPath.isDefined) {
      customPath.get + fileNameSpec.toString
    } else {
      partDir.toString + fileNameSpec.toString
    }
    currentPath
  }

  /**
   * Opens a new OutputWriter given a partition key and/or a bucket id.
   * If bucket id is specified, we will append it to the end of the file name, but before the
   * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
   *
   * @param partitionValues    the partition which all tuples being written by this OutputWriter
   *                           belong to
   * @param bucketId           the bucket which all tuples being written by this OutputWriter belong to
   * @param closeCurrentWriter close and release resource for current writer
   */
  protected def renewCurrentWriter(
                                    partitionValues: Option[InternalRow],
                                    bucketId: Option[Int],
                                    closeCurrentWriter: Boolean): Unit = {

    recordsInFile = 0
    if (closeCurrentWriter) {
      releaseCurrentWriter()
    }

    val partDir = partitionValues.map(getPartitionPath(_))
    partDir.foreach(updatedPartitions.add)

    val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

    // The prefix and suffix must be in a form that matches our bucketing format. See BucketingUtils
    // for details. The prefix is required to represent bucket id when writing Hive-compatible
    // bucketed table.
    val prefix = bucketId match {
      case Some(id) => description.bucketSpec.get.bucketFileNamePrefix(id)
      case _ => ""
    }
    val suffix = f"$bucketIdStr.c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val fileNameSpec = FileNameSpec(prefix, suffix)

    val customPath = partDir.flatMap { dir =>
      description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
    }
    val currentPath = if (customPath.isDefined) {
      committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, fileNameSpec)
    } else {
      committer.newTaskTempFile(taskAttemptContext, partDir, fileNameSpec)
    }

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)
    currentWriter.asInstanceOf[OmniOrcOutputWriter]
      .initialize(description.allColumns, description.dataColumns)
    statsTrackers.foreach(_.newFile(currentPath))
  }

  /**
   * Open a new output writer when number of records exceeding limit.
   *
   * @param partitionValues the partition which all tuples being written by this `OutputWriter`
   *                        belong to
   * @param bucketId        the bucket which all tuples being written by this `OutputWriter` belong to
   */
  protected def renewCurrentWriterIfTooManyRecords(
                                                    partitionValues: Option[InternalRow],
                                                    bucketId: Option[Int]): Unit = {
    // Exceeded the threshold in terms of the number of records per file.
    // Create a new file by increasing the file counter.
    fileCounter += 1
    assert(fileCounter < MAX_FILE_COUNTER,
      s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")
    renewCurrentWriter(partitionValues, bucketId, closeCurrentWriter = true)
  }

  /**
   * Writes the given record with current writer.
   *
   * @param record The record to write
   */
  protected def writeRecord(record: InternalRow, startPos: Long, endPos: Long): Unit = {
    // TODO After add OmniParquetOutPutWriter need extract
    //  a abstract interface named OmniOutPutWriter
    assert(currentWriter.isInstanceOf[OmniOrcOutputWriter])
    currentWriter.asInstanceOf[OmniOrcOutputWriter].spiltWrite(record, startPos, endPos)

    statsTrackers.foreach(_.newRow(currentWriter.path, record))
    recordsInFile += record.asInstanceOf[OmniInternalRow].batch.numRows()
  }
}

/**
 * Dynamic partition writer with single writer, meaning only one writer is opened at any time for
 * writing. The records to be written are required to be sorted on partition and/or bucket
 * column(s) before writing.
 */
class OmniDynamicPartitionDataSingleWriter(
                                            description: WriteJobDescription,
                                            taskAttemptContext: TaskAttemptContext,
                                            committer: FileCommitProtocol,
                                            customMetrics: Map[String, SQLMetric] = Map.empty)
  extends OmniBaseDynamicPartitionDataWriter(description, taskAttemptContext, committer,
    customMetrics) {

  private var currentPartitionValues: Option[UnsafeRow] = None
  private var currentBucketId: Option[Int] = None

  override def write(record: InternalRow): Unit = {
    assert(record.isInstanceOf[OmniInternalRow])
    splitWrite(record)
  }

  private def splitWrite(omniInternalRow: InternalRow): Unit = {
    val batch = omniInternalRow.asInstanceOf[OmniInternalRow].batch
    val numRows = batch.numRows()
    var lastIndex = 0
    for (i <- 0 until numRows) {
      val record = batch.getRow(i)
      val nextPartitionValues = if (isPartitioned) Some(getPartitionValues(record)) else None
      val nextBucketId = if (isBucketed) Some(getBucketId(record)) else None

      if (currentPartitionValues != nextPartitionValues || currentBucketId != nextBucketId) {
        val isFilePathSame = getPartitionPath(currentPartitionValues,
          currentBucketId) == getPartitionPath(nextPartitionValues, nextBucketId)
        if (!isFilePathSame) {
          // See a new partition or bucket - write to a new partition dir (or a new bucket file).
          if (isPartitioned && currentPartitionValues != nextPartitionValues) {
            currentPartitionValues = Some(nextPartitionValues.get.copy())
            statsTrackers.foreach(_.newPartition(currentPartitionValues.get))
          }
          if (isBucketed) {
            currentBucketId = nextBucketId
          }

          fileCounter = 0
          if (i != 0) {
            writeRecord(omniInternalRow, lastIndex, i)
            lastIndex = i
          }
          renewCurrentWriter(currentPartitionValues, currentBucketId, closeCurrentWriter = true)
        }
      } else if (
        description.maxRecordsPerFile > 0 &&
          recordsInFile >= description.maxRecordsPerFile
      ) {
        if (i != 0) {
          writeRecord(omniInternalRow, lastIndex, i)
          lastIndex = i
        }
        renewCurrentWriterIfTooManyRecords(currentPartitionValues, currentBucketId)
      }
    }
    if (lastIndex < batch.numRows()) {
      writeRecord(omniInternalRow, lastIndex, numRows)
    }
  }
}



