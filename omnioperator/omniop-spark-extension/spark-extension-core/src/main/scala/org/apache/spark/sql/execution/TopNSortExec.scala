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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

/**
 * Performs topN sort
 *
 * @param strictTopN when true it strictly returns n results. This param distinguishes
 *                   [[RowNumber]] from [[Rank]]. [[RowNumber]] corresponds to true
 *                   and [[Rank]] corresponds to false.
 * @param partitionSpec partitionSpec of [[org.apache.spark.sql.execution.window.WindowExec]]
 * @param sortOrder orderSpec of [[org.apache.spark.sql.execution.window.WindowExec]]
 * */
case class TopNSortExec(
                         n: Int,
                         strictTopN: Boolean,
                         partitionSpec: Seq[Expression],
                         sortOrder: Seq[SortOrder],
                         global: Boolean,
                         child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("unsupported topn sort exec")
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
