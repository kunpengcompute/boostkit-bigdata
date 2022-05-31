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

package org.apache.spark.sql.execution.datasources

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.{SparkUpgradeException, TaskContext, Partition => RDDPartition}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.{DataIoAdapter, NdpUtils, PageCandidate, PageToColumnar, PushDownManager, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.ndp.{NdpConf, PushDownInfo}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * An RDD that scans a list of file partitions.
 */
class FileScanRDDPushDown(
    @transient private val sparkSession: SparkSession,
    @transient val filePartitions: Seq[FilePartition],
    requiredSchema: StructType,
    output: Seq[Attribute],
    dataSchema: StructType,
    pushDownOperators: PushDownInfo,
    partitionColumns: Seq[Attribute],
    isColumnVector: Boolean,
    fileFormat: FileFormat)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  var columnOffset = -1
  var filterOutput : Seq[Attribute] = Seq()
  val maxFailedTimes = NdpConf.getMaxFailedTimes(sparkSession).toInt
  if (pushDownOperators.filterExecutions != null && pushDownOperators.filterExecutions.size > 0) {
    columnOffset = NdpUtils.getColumnOffset(dataSchema,
      pushDownOperators.filterExecutions(0).output)
    filterOutput = pushDownOperators.filterExecutions(0).output
  } else if (pushDownOperators.aggExecutions != null && pushDownOperators.aggExecutions.size > 0) {
    columnOffset = NdpUtils.getColumnOffsetByAggExeInfo(dataSchema,
      pushDownOperators.aggExecutions)
  } else {
    columnOffset = NdpUtils.getColumnOffset(dataSchema, output)
    filterOutput = output
  }
  var fpuMap = pushDownOperators.fpuHosts
  var fpuList : Seq[String] = Seq()
  for (key <- fpuMap.keys) {
    fpuList = fpuList :+ key
  }

  val filterExecution = pushDownOperators.filterExecutions
  val aggExecution = pushDownOperators.aggExecutions
  val limitExecution = pushDownOperators.limitExecution
  var sqlAggExpressions : Seq[Expression] = Seq()
  var sqlGroupExpressions : Seq[Expression] = Seq()
  var expressionMaps: scala.collection.mutable.Map[String, Seq[Expression]] =
    scala.collection.mutable.Map[String, Seq[Expression]]()
  var aggMaps: scala.collection.mutable.Map[String,
    scala.collection.mutable.Map[String, Seq[Expression]]] =
    scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Seq[Expression]]]()
  var projectId = 0
  val expressions: util.ArrayList[Object] = new util.ArrayList[Object]()
  private val timeOut = NdpConf.getNdpZookeeperTimeout(sparkSession)
  private val parentPath = NdpConf.getNdpZookeeperPath(sparkSession)
  private val zkAddress = NdpConf.getNdpZookeeperAddress(sparkSession)

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val pageToColumnarClass = new PageToColumnar(requiredSchema, output)

    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead
      private val getBytesReadCallback =
      SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null
      private[this] val sdiHosts = split.asInstanceOf[FilePartition].sdi
      val dataIoClass = new DataIoAdapter()

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        val hasNext = currentIterator != null && currentIterator.hasNext
        if (hasNext) {
          hasNext
        } else {
          val tmp: util.ArrayList[Object] = new util.ArrayList[Object]()
          var hasnextIterator = false
          try {
            hasnextIterator = dataIoClass.hasNextIterator(tmp, pageToColumnarClass,
              currentFile, isColumnVector)
          } catch {
            case e : Exception =>
              throw e
          }
          val ret = if (hasnextIterator && tmp.size() > 0) {
            currentIterator = tmp.asScala.iterator
            hasnextIterator
          } else {
            nextIterator()
          }
          ret
        }
      }
      def next(): Object = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        // don't need to run this `if` for every record.
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          incTaskInputMetricsBytesRead()
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          // too costly to update every record
          if (inputMetrics.recordsRead %
            SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
            incTaskInputMetricsBytesRead()
          }
          inputMetrics.incRecordsRead(1)
        }
        nextElement
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          // logInfo(s"Reading File $currentFile")
          InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)
          val pageCandidate = new PageCandidate(currentFile.filePath, currentFile.start,
            currentFile.length, columnOffset, sdiHosts,
            fileFormat.toString, maxFailedTimes)
          val dataIoPage = dataIoClass.getPageIterator(pageCandidate, output,
            partitionColumns, filterOutput, pushDownOperators)
          currentIterator = pageToColumnarClass.transPageToColumnar(dataIoPage,
            isColumnVector).asScala.iterator
          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              val message = "Parquet column cannot be converted in " +
                s"file ${currentFile.filePath}. Column: ${e.getColumn}, " +
                s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
              throw new QueryExecutionException(message, e)
            case e: ParquetDecodingException =>
              if (e.getCause.isInstanceOf[SparkUpgradeException]) {
                throw e.getCause
              } else if (e.getMessage.contains("Can not read value at")) {
                val message = "Encounter error while reading parquet files. " +
                  "One possible cause: Parquet column cannot be converted in the " +
                  "corresponding files. Details: "
                throw new QueryExecutionException(message, e)
              }
              throw e
          }
        } else {
          currentFile = null
          InputFileBlockHolder.unset()
          false
        }
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = {
    filePartitions.map { partitionFile => {
      val retHost = mutable.HashMap.empty[String, Long]
      partitionFile.files.foreach { partitionMap => {
        partitionMap.locations.foreach {
          sdiKey => {
            retHost(sdiKey) = retHost.getOrElse(sdiKey, 0L) + partitionMap.length
            sdiKey
        }}
      }}

      val datanode = retHost.toSeq.sortWith((x, y) => x._2 > y._2).toIterator
      var mapNum = 0
      if (fpuMap == null) {
        val pushDownManagerClass = new PushDownManager()
        fpuMap = pushDownManagerClass.getZookeeperData(timeOut, parentPath, zkAddress)
      }
      while (datanode.hasNext && mapNum < maxFailedTimes) {
        val datanodeStr = datanode.next()._1
        if (fpuMap.contains(datanodeStr)) {
          val partitioned = fpuMap(datanodeStr)
          if (!"".equalsIgnoreCase(partitionFile.sdi)) {
            partitionFile.sdi ++= ","
          }
          partitionFile.sdi ++= partitioned
        } else if (datanodeStr.equalsIgnoreCase("localhost")) {
          val index = NdpUtils.getFpuHosts(fpuList.size)
          val partitioned = fpuList(index)
          if (!"".equalsIgnoreCase(partitionFile.sdi)) {
            partitionFile.sdi ++= ","
          }
          partitionFile.sdi ++= partitioned
        }
        mapNum = mapNum + 1
      }
      partitionFile.sdi
    }}.toArray
    filePartitions.toArray
  }

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    split.asInstanceOf[FilePartition].preferredLocations()
  }
}
