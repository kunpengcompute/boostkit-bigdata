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

package org.apache.spark.sql.execution

import java.util.concurrent._

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.sparkTypeToOmniType
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal
import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike}
import org.apache.spark.sql.execution.joins.{EmptyHashedRelation, HashedRelationBroadcastMode, HashedRelationWithAllNullKeys}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{SparkFatalException, ThreadUtils}


class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
  extends BroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan) with BroadcastExchangeLike {
  import ColumnarBroadcastExchangeExec._

  override def nodeName: String = "OmniColumnarBroadcastExchange"
  override def supportsColumnar: Boolean = true

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  def buildCheck(): Unit = {
    child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
  }

  @transient
  override lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      sqlContext.sparkSession, ColumnarBroadcastExchangeExec.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(runId.toString, s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val nullBatchCount = sparkContext.longAccumulator("nullBatchCount")
        val beforeCollect = System.nanoTime()
        val numRows = longMetric("numOutputRows")
        val dataSize = longMetric("dataSize")
        var nullRelationFlag = false
        // Use executeCollect/executeCollectIterator to avoid conversion to Scala types
        val input = child.executeColumnar().mapPartitions { iter =>
          val serializer = VecBatchSerializerFactory.create()
          mode match {
            case hashRelMode: HashedRelationBroadcastMode =>
              nullRelationFlag = hashRelMode.isNullAware
            case _ =>
          }
          new Iterator[Array[Byte]] {
            override def hasNext: Boolean = {
              iter.hasNext
            }

            override def next(): Array[Byte] = {
              val batch = iter.next()
              var index = 0
              // When nullRelationFlag is true, it means anti-join
              // Only one column of data is involved in the anti-
              if(nullRelationFlag && batch.numCols()>0) {
                val vec = batch.column(0)
                if (vec.hasNull) {
                  try {
                    nullBatchCount.add(1)
                  } catch {
                    case e : Exception =>
                      logError(s"compute null BatchCount error : ${e.getMessage}.")
                  }
                }
              }
              val vectors = transColBatchToOmniVecs(batch)
              val vecBatch = new VecBatch(vectors, batch.numRows())
              numRows += vecBatch.getRowCount
              val vecBatchSer = serializer.serialize(vecBatch)
              dataSize += vecBatchSer.length
              // close omni vec
              vecBatch.releaseAllVectors()
              vecBatch.close()
              vecBatchSer
            }
          }
        }.collect()
        val relation = new ColumnarHashedRelation
        relation.converterData(mode, nullBatchCount.value, input)
        val numOutputRows = numRows.value
        if (numOutputRows >= MAX_BROADCAST_TABLE_ROWS) {
          throw new SparkException(s"Cannot broadcast the table over " +
            s"$MAX_BROADCAST_TABLE_ROWS rows: $numOutputRows rows")
        }

        val beforeBroadcast = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeCollect)

        // Broadcast the relation
        val broadcasted: broadcast.Broadcast[Any] = sparkContext.broadcast(relation)
        longMetric("broadcastTime") += NANOSECONDS.toMillis(
          System.nanoTime() - beforeBroadcast)
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.trySuccess(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
              "worker nodes. As a workaround, you can either disable broadcast by setting " +
              s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
              s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause))
          promise.tryFailure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.tryFailure(ex)
          throw ex
        case e: Throwable =>
          promise.tryFailure(e)
          throw e
      }
    }
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }
}

class ColumnarHashedRelation extends Serializable {
  var relation: Any = _
  var buildData: Array[Array[Byte]] = new Array[Array[Byte]](0)

  def converterData(mode: BroadcastMode, nullVecCount: Long, array: Array[Array[Byte]]): Unit = {
    if (mode.isInstanceOf[HashedRelationBroadcastMode] && array.isEmpty) {
      relation = EmptyHashedRelation
    }
    if (nullVecCount >= 1) {
      relation = HashedRelationWithAllNullKeys
    }
    buildData = array
  }
}

object ColumnarBroadcastExchangeExec {
  // Since the maximum number of keys that BytesToBytesMap supports is 1 << 29,
  // and only 70% of the slots can be used before growing in HashedRelation,
  // here the limitation should not be over 341 million.
  val MAX_BROADCAST_TABLE_ROWS = (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong

  val MAX_BROADCAST_TABLE_BYTES = 8L << 30

  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
