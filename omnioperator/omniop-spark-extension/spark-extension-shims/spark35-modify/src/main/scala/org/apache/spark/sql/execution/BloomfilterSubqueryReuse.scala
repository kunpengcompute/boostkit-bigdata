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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

case class SubqueryWrapper (
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression with LeafLike[Expression] {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = true
  override def toString: String = s"${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): SubqueryWrapper = copy(plan = plan)
  final override def nodePatternsInternal: Seq[TreePattern] = Seq(SUBQUERY_WRAPPER)

  override lazy val canonicalized: Expression = {
    SubqueryWrapper(plan.canonicalized.asInstanceOf[BaseSubqueryExec], ExprId(0))
  }

  @transient private var result: Array[Any] = null
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = if (plan.output.length > 1) {
      rows.asInstanceOf[Array[Any]]
    } else {
      rows.map(_.get(0, plan.schema.fields.head.dataType))
    }

    updated = true
  }

  override def eval(input: InternalRow): Array[Any] = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}

/**
 * A proxy class for [[SubqueryBroadcastExec]], because [[SubqueryBroadcastExec]] doesn't support
 * `execute`. The proxy class will convert the calling from `execute` to `executeCollect`.
 */
case class BroadcastExchangeExecProxy(
    child: SparkPlan, output: Seq[Attribute]) extends UnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  def doExecute(): RDD[InternalRow] = {
    val broadcastedValues = child.executeCollect()
    session.sparkContext.parallelize(broadcastedValues, 1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BroadcastExchangeExecProxy =
    copy(child = newChild)
}