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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarBaseLimitExec extends LimitExec {

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val hasInput = iter.hasNext
      if (hasInput) {
        new Iterator[ColumnarBatch] {
          var rowCount = 0
          override def hasNext: Boolean = {
            val hasNext = iter.hasNext
            hasNext && (rowCount < limit)
          }

          override def next(): ColumnarBatch = {
            val output = iter.next()
            val preRowCount = rowCount
            rowCount += output.numRows
            if (rowCount > limit) {
              val newSize = limit - preRowCount
              output.setNumRows(newSize)
            }
            output
          }
        }
      } else {
        Iterator.empty
      }
    }
  }

  protected override def doExecute() = {
    throw new UnsupportedOperationException("This operator doesn't support doExecute()")
  }
}

case class ColumnarLocalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def nodeName: String = "OmniColumnarLocalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class ColumnarGlobalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def nodeName: String = "OmniColumnarGlobalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
