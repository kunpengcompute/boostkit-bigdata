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

package org.apache.spark.sql.execution

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.sparkTypeToOmniType

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Physical plan for returning a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.  If a larger number of partitions
 * is requested, it will stay at the current number of partitions.
 *
 * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
 * this may result in your computation taking place on fewer nodes than
 * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
 * you see ShuffleExchange. This will add a shuffle step, but means the
 * current upstream partitions will be executed in parallel (per whatever
 * the current partitioning is).
 */
case class ColumnarCoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {

  override def nodeName: String = "ColumnarCoalesceExec"

  override def supportsColumnar: Boolean = true

  def buildCheck(): Unit = {
    child.output.foreach(attr => sparkTypeToOmniType(attr.dataType, attr.metadata))
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("ColumnarCoalesceExec operator doesn't support doExecute().")
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new ColumnarCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarCoalesceExec =
    copy(child = newChild)
}

object ColumnarCoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[ColumnarBatch](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}