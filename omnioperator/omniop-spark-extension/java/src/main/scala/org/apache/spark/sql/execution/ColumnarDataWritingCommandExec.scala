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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A physical operator that executes the run method of a `ColumnarDataWritingCommand` and
 * saves the result to prevent multiple executions.
 *
 * @param cmd   the `ColumnarDataWritingCommand` this operator will run.
 * @param child the physical plan child ran by the `DataWritingCommand`.
 */
case class ColumnarDataWritingCommandExec(@transient cmd: DataWritingCommand, child: SparkPlan)
  extends UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  protected[sql] lazy val sideEffectResult: Seq[ColumnarBatch] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(session, child)

    rows.map(converter(_).asInstanceOf[ColumnarBatch])
  }

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "OmniExecute " + cmd.nodeName

  // override the default one, otherwise the `cmd.nodeName` will appear twice from simpleString
  override def argString(maxFields: Int): String = cmd.argString(maxFields)

  override def executeCollect(): Array[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support executeCollect")
  }

  override def executeToIterator(): Iterator[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support executeToIterator")
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support executeTake")
  }

  override def executeTail(limit: Int): Array[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support executeTail")
  }

  override def supportsColumnar: Boolean = true

  def buildCheck(): Unit = {
    child.output.foreach(exp =>
      if (exp.dataType != TimestampType) {
        sparkTypeToOmniType(exp.dataType, exp.metadata)
      } else {
        throw new UnsupportedOperationException("This operator doesn't support dataType TimestampType")
      }
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support doExecute")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.parallelize(sideEffectResult, 1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarDataWritingCommandExec =
    copy(child = newChild)

}