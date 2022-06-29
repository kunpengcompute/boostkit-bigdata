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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.Optional
import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER
import nova.hetu.omniruntime.operator.config.OperatorConfig
import nova.hetu.omniruntime.operator.join.{OmniHashBuilderWithExprOperatorFactory, OmniLookupJoinWithExprOperatorFactory}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarShuffledHashJoinExec(
                                         leftKeys: Seq[Expression],
                                         rightKeys: Seq[Expression],
                                         joinType: JoinType,
                                         buildSide: BuildSide,
                                         condition: Option[Expression],
                                         left: SparkPlan,
                                         right: SparkPlan)
  extends HashJoin with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "lookupAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni lookup addInput"),
    "lookupGetOutputTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni lookup getOutput"),
    "lookupCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni lookup codegen"),
    "buildAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni build addInput"),
    "buildGetOutputTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni build getOutput"),
    "buildCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext,
      "time in omni build codegen"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "build side input data size")
  )

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = "OmniColumnarShuffledHashJoin"

  override def output: Seq[Attribute] = super[ShuffledJoin].output

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil
    case _ => super.outputOrdering
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(
      iter,
      buildBoundKeys,
      taskMemoryManager = context.taskMemoryManager(),
      // Full outer join needs support for NULL key in HashedRelation.
      allowsNullKey = joinType == FullOuter)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
      s"in ${this.nodeName}")
  }

  def buildCheck(): Unit = {
    if ("INNER" != joinType.sql) {
      throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
        s"in ${this.nodeName}")
    }
    val buildTypes = new Array[DataType](buildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    streamedKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(streamedOutput.map(_.toAttribute)))
    }.toArray
  }

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val buildAddInputTime = longMetric("buildAddInputTime")
    val buildCodegenTime = longMetric("buildCodegenTime")
    val buildGetOutputTime = longMetric("buildGetOutputTime")
    val lookupAddInputTime = longMetric("lookupAddInputTime")
    val lookupCodegenTime = longMetric("lookupCodegenTime")
    val lookupGetOutputTime = longMetric("lookupGetOutputTime")
    val buildDataSize = longMetric("buildDataSize")

    if ("INNER" != joinType.sql) {
      throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
        s"in ${this.nodeName}")
    }
    val buildTypes = new Array[DataType](buildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val buildOutputCols = buildOutput.indices.toArray
    val buildJoinColsExp = buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray

    val buildOutputTypes = buildTypes

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = streamedOutput.indices.toArray
    val probeHashColsExp = streamedKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(streamedOutput.map(_.toAttribute)))
    }.toArray

    streamedPlan.executeColumnar.zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) =>
        val filter: Optional[String] = condition match {
          case Some(expr) =>
            Optional.of(OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
              OmniExpressionAdaptor.getExprIdMap((streamedOutput ++ buildOutput).
                map(_.toAttribute))))
          case _ => Optional.empty()
        }
        val startBuildCodegen = System.nanoTime()
        val buildOpFactory = new OmniHashBuilderWithExprOperatorFactory(buildTypes,
          buildJoinColsExp, filter, 1, new OperatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP))
        val buildOp = buildOpFactory.createOperator()
        buildCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildCodegen)

        while (buildIter.hasNext) {

          val cb = buildIter.next()
          val vecs = transColBatchToOmniVecs(cb, false)
          for (i <- 0 until vecs.length) {
            buildDataSize += vecs(i).getRealValueBufCapacityInBytes
            buildDataSize += vecs(i).getRealNullBufCapacityInBytes
            buildDataSize += vecs(i).getRealOffsetBufCapacityInBytes
          }
          val startBuildInput = System.nanoTime()
          buildOp.addInput(new VecBatch(vecs, cb.numRows()))
          buildAddInputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildInput)
        }

        val startBuildGetOp = System.nanoTime()
        buildOp.getOutput
        buildGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildGetOp)

        val startLookupCodegen = System.nanoTime()
        val lookupOpFactory = new OmniLookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols,
          probeHashColsExp, buildOutputCols, buildOutputTypes, OMNI_JOIN_TYPE_INNER, buildOpFactory,
          new OperatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP))
        val lookupOp = lookupOpFactory.createOperator()
        lookupCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupCodegen)

        // close operator
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          buildOp.close()
          lookupOp.close()
          buildOpFactory.close()
          lookupOpFactory.close()
        })

        val resultSchema = this.schema
        val reverse = this.output != (streamedPlan.output ++ buildPlan.output)
        var left = 0
        var leftLen = streamedPlan.output.size
        var right = streamedPlan.output.size
        var rightLen = output.size
        if (reverse) {
          left = streamedPlan.output.size
          leftLen = output.size
          right = 0
          rightLen = streamedPlan.output.size
        }

        new Iterator[ColumnarBatch] {
          private var results: java.util.Iterator[VecBatch] = _
          var res: Boolean = true

          override def hasNext: Boolean = {
            while ((results == null || !res) && streamIter.hasNext) {
              val batch = streamIter.next()
              val input = transColBatchToOmniVecs(batch)
              val vecBatch = new VecBatch(input, batch.numRows())
              val startLookupInput = System.nanoTime()
              lookupOp.addInput(vecBatch)
              lookupAddInputTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupInput)

              val startLookupGetOp = System.nanoTime()
              results = lookupOp.getOutput
              res = results.hasNext
              lookupGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupGetOp)

            }
            if (results == null) {
              false
            } else {
              if (!res) {
                false
              } else {
                val startLookupGetOp = System.nanoTime()
                res = results.hasNext
                lookupGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupGetOp)
                res
              }
            }

          }

          override def next(): ColumnarBatch = {
            val startLookupGetOp = System.nanoTime()
            val result = results.next()
            res = results.hasNext
            lookupGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupGetOp)
            val resultVecs = result.getVectors
            val vecs = OmniColumnVector
              .allocateColumns(result.getRowCount, resultSchema, false)
            var index = 0
            for (i <- left until leftLen) {
              val v = vecs(index)
              v.reset()
              v.setVec(resultVecs(i))
              index += 1
            }
            for (i <- right until rightLen) {
              val v = vecs(index)
              v.reset()
              v.setVec(resultVecs(i))
              index += 1
            }
            numOutputRows += result.getRowCount
            numOutputVecBatchs += 1
            new ColumnarBatch(vecs.toArray, result.getRowCount)
          }
        }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[1]);", forceInline = true)
    HashedRelationInfo(relationTerm, keyIsUnique = false, isEmpty = false)
  }
}
