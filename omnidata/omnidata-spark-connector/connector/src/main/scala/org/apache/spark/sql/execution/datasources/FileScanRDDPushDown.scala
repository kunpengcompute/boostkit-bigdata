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
import org.apache.spark.executor.InputMetrics
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.{DataIoAdapter, NdpUtils, PageCandidate, PageToColumnar, PushDownManager, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BasePredicate, Expression, Predicate, UnsafeProjection}
import org.apache.spark.sql.execution.{QueryExecutionException, RowToColumnConverter}
import org.apache.spark.sql.execution.ndp.{FilterExeInfo, NdpConf, PushDownInfo}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf.ORC_IMPLEMENTATION
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

import java.io.{FileNotFoundException, IOException}
import scala.util.Random


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
    fileFormat: FileFormat,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    partialCondition: Boolean,
    partialPdRate: Double,
    zkPdRate: Double,
    partialChildOutput: Seq[Attribute])
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
  val enableOffHeapColumnVector: Boolean = sparkSession.sessionState.conf.offHeapColumnVectorEnabled
  val columnBatchSize: Int = sparkSession.sessionState.conf.columnBatchSize
  val converters = new RowToColumnConverter(StructType.fromAttributes(output))
  private val timeOut = NdpConf.getNdpZookeeperTimeout(sparkSession)
  private val parentPath = NdpConf.getNdpZookeeperPath(sparkSession)
  private val zkAddress = NdpConf.getNdpZookeeperAddress(sparkSession)
  private val taskTimeout = NdpConf.getTaskTimeout(sparkSession)
  private val operatorCombineEnabled = NdpConf.getNdpOperatorCombineEnabled(sparkSession)
  val orcImpl = sparkSession.sessionState.conf.getConf(ORC_IMPLEMENTATION)

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val pageToColumnarClass = new PageToColumnar(requiredSchema, output)
    var iterator : PushDownIterator = null
    if (isPartialPushDown(partialCondition, partialPdRate, zkPdRate)) {
      logDebug("partial push down task on spark")
      val partialFilterCondition = pushDownOperators.filterExecutions.reduce((a, b) => FilterExeInfo(And(a.filter, b.filter), partialChildOutput))
      val predicate = Predicate.create(partialFilterCondition.filter, partialChildOutput)
      predicate.initialize(0)
      iterator = new PartialPushDownIterator(split, context, pageToColumnarClass, predicate)
    } else {
      logDebug("partial push down task on omnidata")
      iterator = new PushDownIterator(split, context, pageToColumnarClass)
    }
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  def isPartialPushDown(partialCondition: Boolean, partialPdRate: Double, zkPdRate: Double): Boolean = {
    var res = false
    val randomNum = Random.nextDouble;
    if (partialCondition && (randomNum > partialPdRate || randomNum > zkPdRate)) {
      res = true
    }
    res
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
        val fMap = pushDownManagerClass.getZookeeperData(timeOut, parentPath, zkAddress)
        val hostMap = mutable.Map[String,String]()
        for (kv <- fMap) {
          hostMap.put(kv._1, kv._2.getDatanodeHost)
        }
        fpuMap = hostMap
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

  class PushDownIterator(split: RDDPartition,
                     context: TaskContext,
                     pageToColumnarClass: PageToColumnar)
    extends Iterator[Object] with AutoCloseable {

    val inputMetrics: InputMetrics = context.taskMetrics().inputMetrics
    val existingBytesRead: Long = inputMetrics.bytesRead
    val getBytesReadCallback: () => Long =
      SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
    def incTaskInputMetricsBytesRead(): Unit = {
      inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
    }

    val files: Iterator[PartitionedFile] = split.asInstanceOf[FilePartition].files.toIterator
    var currentFile: PartitionedFile = null
    var currentIterator: Iterator[Object] = null
    val sdiHosts: String = split.asInstanceOf[FilePartition].sdi
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
          hasnextIterator = dataIoClass.hasNextIterator(tmp, pageToColumnarClass, isColumnVector, output, orcImpl)
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
    def nextIterator(): Boolean = {
      if (files.hasNext) {
        currentFile = files.next()
        // logInfo(s"Reading File $currentFile")
        InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)
        val pageCandidate = new PageCandidate(currentFile.filePath, currentFile.start,
          currentFile.length, columnOffset, sdiHosts,
          fileFormat.toString, maxFailedTimes, taskTimeout,operatorCombineEnabled)
        val dataIoPage = dataIoClass.getPageIterator(pageCandidate, output,
          partitionColumns, filterOutput, pushDownOperators)
        currentIterator = pageToColumnarClass.transPageToColumnar(dataIoPage,
          isColumnVector, dataIoClass.isOperatorCombineEnabled, output, orcImpl).asScala.iterator
        iteHasNext()
      } else {
        unset()
      }
    }

    def iteHasNext(): Boolean = {
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
    }

    def unset(): Boolean = {
      currentFile = null
      InputFileBlockHolder.unset()
      false
    }

    override def close(): Unit = {
      incTaskInputMetricsBytesRead()
      InputFileBlockHolder.unset()
    }
  }

  class PartialPushDownIterator(split: RDDPartition,
                         context: TaskContext,
                         pageToColumnarClass: PageToColumnar,
                         predicate: BasePredicate)
    extends PushDownIterator(split: RDDPartition, context: TaskContext, pageToColumnarClass: PageToColumnar) {

    override def hasNext: Boolean = {
      // Kill the task in case it has been marked as killed. This logic is from
      // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead.
      context.killTaskIfInterrupted()
      (currentIterator != null && currentIterator.hasNext) || nextIterator()
    }

    override def nextIterator(): Boolean = {
      if (files.hasNext) {
        currentFile = files.next()
        InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)
        predicate.initialize(0)
        val toUnsafe = UnsafeProjection.create(output, filterOutput)
        if (isColumnVector) {
          currentIterator = readCurrentFile().asInstanceOf[Iterator[ColumnarBatch]]
            .map { c =>
              val rowIterator = c.rowIterator().asScala
              val ri = rowIterator.filter { row =>
                val r = predicate.eval(row)
                r
              }

              val projectRi = ri.map(toUnsafe)
              val vectors: Seq[WritableColumnVector] = if (enableOffHeapColumnVector) {
                OffHeapColumnVector.allocateColumns(columnBatchSize, StructType.fromAttributes(output))
              } else {
                OnHeapColumnVector.allocateColumns(columnBatchSize, StructType.fromAttributes(output))
              }
              val cb: ColumnarBatch = new ColumnarBatch(vectors.toArray)

              TaskContext.get().addTaskCompletionListener[Unit] { _ =>
                cb.close()
              }

              cb.setNumRows(0)
              vectors.foreach(_.reset())
              var rowCount = 0
              while (rowCount < columnBatchSize && projectRi.hasNext) {
                val row = projectRi.next()
                converters.convert(row, vectors.toArray)
                rowCount += 1
              }
              cb.setNumRows(rowCount)
              cb
            }
        } else {
          val rowIterator = readCurrentFile().filter { row =>
            val r = predicate.eval(row)
            r
          }
          currentIterator = rowIterator.map(toUnsafe)
        }
        iteHasNext()
      } else {
        unset()
      }
    }

    private def readCurrentFile(): Iterator[InternalRow] = {
      try {
        readFunction(currentFile)
      } catch {
        case e: FileNotFoundException =>
          throw new FileNotFoundException(
            e.getMessage + "\n" +
              "It is possible the underlying files have been updated. " +
              "You can explicitly invalidate the cache in Spark by " +
              "running 'REFRESH TABLE tableName' command in SQL or " +
              "by recreating the Dataset/DataFrame involved.")
      }
    }
  }
}
