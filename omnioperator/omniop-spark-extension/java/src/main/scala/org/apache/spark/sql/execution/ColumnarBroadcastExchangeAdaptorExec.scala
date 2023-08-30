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

import nova.hetu.omniruntime.vector.Vec
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer

case class ColumnarBroadcastExchangeAdaptorExec(child: SparkPlan, numPartitions: Int)
  extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = UnknownPartitioning(numPartitions)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows: SQLMetric = longMetric("numOutputRows")
    val numOutputBatches: SQLMetric = longMetric("numOutputBatches")
    val processTime: SQLMetric = longMetric("processTime")
    val inputRdd: BroadcastColumnarRDD = BroadcastColumnarRDD(
      sparkContext,
      metrics,
      numPartitions,
      child.executeBroadcast(),
      StructType.fromAttributes(child.output))
    inputRdd.mapPartitions { batches =>
      ColumnarBatchToInternalRow.convert(output, batches, numOutputRows, numOutputBatches, processTime)
    }
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_datatoarrowcolumnar"))

  override protected def withNewChildInternal(newChild: SparkPlan):
    ColumnarBroadcastExchangeAdaptorExec = copy(child = newChild)
}