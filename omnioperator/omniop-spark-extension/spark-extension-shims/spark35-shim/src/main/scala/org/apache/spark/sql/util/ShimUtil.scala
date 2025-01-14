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
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Count, Max, Min, Sum}
import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BinaryOperator, Cast, Divide, EvalMode, Expression, Multiply, Remainder, Subtract}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, IntegerType, LongType, NullType, StringType}

import java.util.{Locale, Properties}

object ShimUtil {

  def isSupportDataWriter: Boolean = false

  def createCTERelationRef(cteId: Long, resolved: Boolean, output: Seq[Attribute], isStreaming: Boolean,
                           tatsOpt: Option[Statistics] = None): CTERelationRef = {
    CTERelationRef(cteId, resolved, output, isStreaming, tatsOpt)
  }

  def getPartitionedFilePath(f: PartitionedFile): Path = {
    f.toPath
  }

  def createTaskContext(stageId: Int, stageAttemptNumber: Int, partitionId: Int, taskAttemptId: Long, attemptNumber: Int,
                        numPartitions: Int, taskMemoryManager: TaskMemoryManager, localProperties: Properties, metricsSystem: MetricsSystem,
                        taskMetrics: TaskMetrics = TaskMetrics.empty, cpus: Int = SparkEnv.get.conf.get(config.CPUS_PER_TASK),
                        resources: Map[String, ResourceInformation] = Map.empty): TaskContextImpl = {
    new TaskContextImpl(stageId, stageAttemptNumber, partitionId, taskAttemptId, attemptNumber, numPartitions, taskMemoryManager, localProperties, metricsSystem, taskMetrics, cpus, resources)
  }

  def unsupportedEvalModeCheck(expr: Expression): Unit = {
    expr match {
      case add: Add if add.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $add")
      case sub: Subtract if sub.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $sub")
      case mult: Multiply if mult.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $mult")
      case divide: Divide if divide.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $divide")
      case mod: Remainder if mod.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $mod")
      case sum: Sum if sum.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $sum")
      case avg: Average if avg.evalMode == EvalMode.TRY =>
        throw new UnsupportedOperationException(s"Unsupported EvalMode TRY for $avg")
      case _ =>
    }
  }

  def binaryOperatorAdjust(expr: BinaryOperator, returnDataType: DataType): (Expression, Expression) = {
    import scala.math.{max, min}
    def bounded(precision: Int, scale: Int): DecimalType = {
      DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
    }

    def widerDecimalType(d1: DecimalType, d2: DecimalType): Tuple2[DecimalType, Boolean] = {
      getWiderDecimalType(d1.precision, d1.scale, d2.precision, d2.scale)
    }

    def getWiderDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): Tuple2[DecimalType, Boolean] = {
      val scale = max(s1, s2)
      val range = max(p1 - s1, p2 - s2)
      (bounded(range + scale, scale), range + scale > MAX_PRECISION)
    }

    def decimalTypeCast(expr: Expression, d: DecimalType, widerType: DecimalType, returnType: DecimalType, isOverPrecision: Boolean): Expression = {
      if (isOverPrecision && d.scale <= returnType.scale) {
        if (returnType.precision - returnType.scale < d.precision - d.scale) {
          return expr
        }
        Cast (expr, returnDataType)
      } else {
        Cast (expr, widerType)
      }
    }

    if (expr.left.dataType.isInstanceOf[DecimalType] && expr.right.dataType.isInstanceOf[DecimalType]) {
      val leftDataType = expr.left.dataType.asInstanceOf[DecimalType]
      val rightDataType = expr.right.dataType.asInstanceOf[DecimalType]
      val returnType = returnDataType.asInstanceOf[DecimalType]
      val (widerType, isOverPrecision) = widerDecimalType(leftDataType, rightDataType)
      val result = expr match {
        case _: Add | _: Subtract =>
          if (returnType.scale < leftDataType.scale && returnType.scale < rightDataType.scale) {
            val adjustType = DecimalType(returnType.precision, min(leftDataType.scale, rightDataType.scale))
            (Cast(expr.left, adjustType), Cast(expr.right, adjustType))
          } else {
            (Cast(expr.left, returnDataType), Cast(expr.right, returnDataType))
          }
        case _: Multiply | _: Divide | _: Remainder =>
          val newLeft = decimalTypeCast(expr.left, leftDataType, widerType, returnType, isOverPrecision)
          val newRight = decimalTypeCast(expr.right, rightDataType, widerType, returnType, isOverPrecision)
          (newLeft, newRight)
        case _ => (expr.left, expr.right)
      }
      return result
    }
    (expr.left, expr.right)
  }

  def unsupportedCastCheck(expr: Expression, cast: Cast): Unit = {
    def doSupportCastToString(dataType: DataType): Boolean = {
      dataType.isInstanceOf[DecimalType] && !SQLConf.get.ansiEnabled || dataType.isInstanceOf[StringType] || dataType.isInstanceOf[IntegerType] ||
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
    Aggregate.supportsHashAggregate(aggregateBufferAttributes)
  }

  def supportsObjectHashAggregate(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    Aggregate.supportsObjectHashAggregate(aggregateExpressions)
  }

  def createCTERelationDef(child: LogicalPlan,
                           id: Long = CTERelationDef.newId,
                           originalPlanWithPredicates: Option[(LogicalPlan, Seq[Expression])] = None,
                           underSubquery: Boolean = false): CTERelationDef = {
    CTERelationDef(child, id, originalPlanWithPredicates, underSubquery)
  }

  def supportsFilterPropagation(a: Aggregate): Boolean = {
    a.groupingExpressions.isEmpty &&
      a.aggregateExpressions.forall(
        !_.exists {
          case ae: AggregateExpression =>
            ae.aggregateFunction match {
              case _: Count | _: Sum | _: Average | _: Max | _: Min => false
              case _ => true
            }
          case _ => false
        }
      )
  }

  def buildBuildSide(buildSide: BuildSide, joinType: JoinType): BuildSide = {
    if (buildSide == BuildLeft && joinType == LeftOuter) {
      BuildRight
    } else if (buildSide == BuildRight && joinType == RightOuter) {
      BuildLeft
    } else {
      buildSide
    }
  }

  def createSortShuffleWriter[K, V, C](handle: BaseShuffleHandle[K, V, C],
                                    mapId: Long,
                                    context: TaskContext,
                                    writeMetrics: ShuffleWriteMetricsReporter,
                                    shuffleExecutorComponents: ShuffleExecutorComponents): ShuffleWriter[K, V] = {
    new SortShuffleWriter(handle, mapId, context, writeMetrics, shuffleExecutorComponents)
  }

  def parseUdfName(originalName: String): String = {
    val nameSplit = originalName.split("\\.")
    // starting with Spark3.4.0, Spark matches 3-layer-namespace
    if (nameSplit.size < 3) nameSplit(0).toLowerCase(Locale.ROOT) else nameSplit(2).toLowerCase(Locale.ROOT)
  }

  def aggFuncUnSupportInputTypeCheck(aggFunc: AggregateFunction): Unit = {
    aggFunc match {
      case Average(expr, _) =>
        expr.dataType match {
          case dt: DecimalType if !DecimalType.is64BitDecimalType(dt) =>
            throw new UnsupportedOperationException(s"Unsupported dataType decimal128 of aggregateFunction: ${aggFunc}")
          case _ =>
        }
      case _ =>
    }
  }
}
