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

package org.apache.spark.sql.util

import org.apache.hadoop.fs.Path
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Count, Max, Min, Sum}
import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryOperator, CastBase, Expression}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, IntegerType, LongType, NullType, StringType}

import java.net.URI
import java.util.{Locale, Properties}

object ShimUtil {

  def isSupportDataWriter: Boolean = false

  def createCTERelationRef(cteId: Long, resolved: Boolean, output: Seq[Attribute], isStreaming: Boolean,
                           tatsOpt: Option[Statistics] = None): CTERelationRef = {
    CTERelationRef(cteId, resolved, output, tatsOpt)
  }

  def getPartitionedFilePath(f: PartitionedFile): Path = {
    new Path(new URI(f.filePath))
  }

  def createTaskContext(stageId: Int, stageAttemptNumber: Int, partitionId: Int, taskAttemptId: Long, attemptNumber: Int,
                        numPartitions: Int, taskMemoryManager: TaskMemoryManager, localProperties: Properties, metricsSystem: MetricsSystem,
                        taskMetrics: TaskMetrics = TaskMetrics.empty, cpus: Int = SparkEnv.get.conf.get(config.CPUS_PER_TASK),
                        resources: Map[String, ResourceInformation] = Map.empty): TaskContextImpl = {
    new TaskContextImpl(stageId, stageAttemptNumber, partitionId, taskAttemptId, attemptNumber, taskMemoryManager, localProperties, metricsSystem, taskMetrics, resources)
  }

  def unsupportedEvalModeCheck(expr: Expression): Unit = {}

  def binaryOperatorAdjust(expr: BinaryOperator, returnDataType: DataType): (Expression, Expression) = {
    (expr.left, expr.right)
  }

  def unsupportedCastCheck(expr: Expression, cast: CastBase): Unit = {
    def doSupportCastToString(dataType: DataType): Boolean = {
      dataType.isInstanceOf[DecimalType] || dataType.isInstanceOf[StringType] || dataType.isInstanceOf[IntegerType] ||
        dataType.isInstanceOf[LongType] || dataType.isInstanceOf[DateType] || dataType.isInstanceOf[DoubleType] ||
        dataType.isInstanceOf[NullType]
    }

    def doSupportCastFromString(dataType: DataType): Boolean = {
      dataType.isInstanceOf[DecimalType] || dataType.isInstanceOf[StringType] || dataType.isInstanceOf[DateType] ||
        dataType.isInstanceOf[IntegerType] || dataType.isInstanceOf[LongType] || dataType.isInstanceOf[DoubleType]
    }

    // support cast(decimal/string/int/long as string)
    if (cast.dataType.isInstanceOf[StringType] && !doSupportCastToString(cast.child.dataType)) {
      throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }

    // support cast(string as decimal/string/date/int/long/double)
    if (!doSupportCastFromString(cast.dataType) && cast.child.dataType.isInstanceOf[StringType]) {
      throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  def supportsHashAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    HashAggregateExec.supportsAggregate(aggregateBufferAttributes)
  }

  def supportsObjectHashAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    ObjectHashAggregateExec.supportsAggregate(aggregateExpressions)
  }

  def createCTERelationDef(child: LogicalPlan,
                           id: Long = CTERelationDef.newId,
                           originalPlanWithPredicates: Option[(LogicalPlan, Seq[Expression])] = None,
                           underSubquery: Boolean = false): CTERelationDef = {
    CTERelationDef(child, id, originalPlanWithPredicates)
  }

  def supportsFilterPropagation(a: Aggregate): Boolean = {
    a.groupingExpressions.isEmpty &&
      a.aggregateExpressions.forall(
        _.find {
          case ae: AggregateExpression =>
            ae.aggregateFunction match {
              case _: Count | _: Sum | _: Average | _: Max | _: Min => false
              case _ => true
            }
          case _ => false
        }.isEmpty
      )
  }

  def buildBuildSide(buildSide: BuildSide, joinType: JoinType): BuildSide = {
    buildSide
  }

  def createSortShuffleWriter[K, V, C](handle: BaseShuffleHandle[K, V, C],
                                       mapId: Long,
                                       context: TaskContext,
                                       writeMetrics: ShuffleWriteMetricsReporter,
                                       shuffleExecutorComponents: ShuffleExecutorComponents): ShuffleWriter[K, V] = {
    new SortShuffleWriter(handle, mapId, context, shuffleExecutorComponents)
  }

  def parseUdfName(originalName: String): String = {
    val nameSplit = originalName.split("\\.")
    if (nameSplit.size == 1) nameSplit(0).toLowerCase(Locale.ROOT) else nameSplit(1).toLowerCase(Locale.ROOT)
  }

  def aggFuncUnSupportInputTypeCheck(aggFunc: AggregateFunction): Unit = {}
}
