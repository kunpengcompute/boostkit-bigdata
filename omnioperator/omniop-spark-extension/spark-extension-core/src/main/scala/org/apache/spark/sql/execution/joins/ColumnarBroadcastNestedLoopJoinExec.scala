/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

import java.util.Optional
import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable
import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, isSimpleColumn, isSimpleColumnForAll}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{getExprIdForProjectList, getIndexArray, getProjectListIndex, pruneOutput, reorderOutputVecs, transColBatchToOmniVecs}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarHashedRelation, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.{MergeIterator, SparkMemoryUtils}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import nova.hetu.omniruntime.constants.JoinType._
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.OmniOperator
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniNestedLoopJoinBuildOperatorFactory, OmniNestedLoopJoinLookupOperatorFactory}
import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory

/**
 * Performs a nested loop join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamedPlan
 * relation is not shuffled.
 */
case class ColumnarBroadcastNestedLoopJoinExec(
                                                left: SparkPlan,
                                                right: SparkPlan,
                                                buildSide: BuildSide,
                                                joinType: JoinType,
                                                condition: Option[Expression],
                                                projectList: Seq[NamedExpression] = Seq.empty) extends JoinCodegenSupport {

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}${condition.get.dataType}"
    } else "None"
    s"""
       |$formattedNodeName
       |$simpleStringWithNodeId
       |${ExplainUtils.generateFieldString("buildInput", buildOutput ++ buildOutput.map(_.dataType))}
       |${ExplainUtils.generateFieldString("streamedInput", streamedOutput ++ streamedOutput.map(_.dataType))}
       |${ExplainUtils.generateFieldString("condition", joinCondStr)}
       |${ExplainUtils.generateFieldString("projectList", projectList.map(_.toAttribute) ++ projectList.map(_.toAttribute).map(_.dataType))}
       |${ExplainUtils.generateFieldString("output", output ++ output.map(_.dataType))}
       |Condition : $condition
       |""".stripMargin
  }

  private val (buildOutput, streamedOutput) = buildSide match {
    case BuildLeft => (left.output, right.output)
    case BuildRight => (right.output, left.output)
  }

  override def leftKeys: Seq[Expression] = Nil

  override def rightKeys: Seq[Expression] = Nil

  private val (streamedPlan, buildPlan) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "lookupAddInputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni lookup addInput"),
    "lookupGetOutputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni lookup getOutput"),
    "lookupCodegenTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni lookup codegen"),
    "buildAddInputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni build addInput"),
    "buildGetOutputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni build getOutput"),
    "buildCodegenTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni build codegen"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
    "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches")
  )

  override protected def withNewChildrenInternal(
                                                  newLeft: SparkPlan, newRight: SparkPlan): ColumnarBroadcastNestedLoopJoinExec =
    copy(left = newLeft, right = newRight)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = "OmniColumnarBroadcastNestedLoopJoin"

  /** only for operator fusion */
  def getBuildOutput: Seq[Attribute] = {
    buildOutput
  }

  def getBuildPlan: SparkPlan = {
    buildPlan
  }

  def getStreamedOutput: Seq[Attribute] = {
    streamedOutput
  }

  def buildCheck(): Unit = {
    joinType match {
      case Inner  =>
      case LeftOuter =>
        require(buildSide == BuildRight, "In left outer join case,buildSide must be BuildRight.")
      case RightOuter =>
        require(buildSide == BuildLeft, "In right outer join case,buildSide must be BuildLeft.")
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
          s"in ${this.nodeName}")
    }
    buildOutput.zipWithIndex.foreach {case (att, i) =>
      OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    condition match {
      case Some(expr) =>
        val filterExpr: String = OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap((streamedOutput ++ buildOutput).map(_.toAttribute)))
        if (!isSimpleColumn(filterExpr)) {
          checkOmniJsonWhiteList(filterExpr, new Array[AnyRef](0))
        }
      case _ => Optional.empty()
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatches = longMetric("numOutputVecBatches")
    val numMergedVecBatches = longMetric("numMergedVecBatches")
    val buildAddInputTime = longMetric("buildAddInputTime")
    val buildCodegenTime = longMetric("buildCodegenTime")
    val buildGetOutputTime = longMetric("buildGetOutputTime")
    val lookupAddInputTime = longMetric("lookupAddInputTime")
    val lookupCodegenTime = longMetric("lookupCodegenTime")
    val lookupGetOutputTime = longMetric("lookupGetOutputTime")

    val buildTypes = new Array[DataType](buildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
    val enableShareBuildOp: Boolean = columnarConf.enableShareBroadcastJoinNestedTable
    val enableJoinBatchMerge: Boolean = columnarConf.enableJoinBatchMerge

    val projectExprIdList = getExprIdForProjectList(projectList)
    // {0}, buildKeys: col1#12
    val buildOutputCols: Array[Int] = joinType match {
      case Inner | LeftOuter | RightOuter =>
        getIndexArray(buildOutput, projectExprIdList)
      case x =>
        throw new UnsupportedOperationException(s"ColumnBroadcastNestedLoopJoin Join-type[$x] is not supported!")
    }
    val prunedBuildOutput = pruneOutput(buildOutput, projectExprIdList)
    val buildOutputTypes = new Array[DataType](prunedBuildOutput.size)
    prunedBuildOutput.zipWithIndex.foreach { case (att, i) =>
      buildOutputTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = getIndexArray(streamedOutput, projectExprIdList) // {0,1}
    val prunedStreamedOutput = pruneOutput(streamedOutput, projectExprIdList)

    val projectListIndex = getProjectListIndex(projectExprIdList, prunedStreamedOutput, prunedBuildOutput)
    val lookupJoinType = OmniExpressionAdaptor.toOmniJoinType(joinType)
    val relation = buildPlan.executeBroadcast[ColumnarHashedRelation]()
    val buildPlanId = buildPlan match {
      case exec: BroadcastQueryStageExec =>
        exec.plan.id
      case _ =>
        buildPlan.id
    }
    streamedPlan.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val filter: Optional[String] = condition match {
        case Some(expr) =>
          Optional.of(OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
            OmniExpressionAdaptor.getExprIdMap((streamedOutput ++ buildOutput).map(_.toAttribute))))
        case _ =>
          Optional.empty()
      }

      def createBuildOpFactoryAndOp(isShared: Boolean): (OmniNestedLoopJoinBuildOperatorFactory, OmniOperator) = {
        val startBuildCodegen = System.nanoTime()
        val opFactory =
          new OmniNestedLoopJoinBuildOperatorFactory(buildTypes, buildOutputCols)
        val op = opFactory.createOperator()
        buildCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildCodegen)

        if (isShared) {
          OmniNestedLoopJoinBuildOperatorFactory.saveNestedLoopJoinBuilderOperatorAndFactory(buildPlanId,
            opFactory, op)
        }
        val deserializer = VecBatchSerializerFactory.create()
        relation.value.buildData.foreach { input =>
          val startBuildInput = System.nanoTime()
          op.addInput(deserializer.deserialize(input))
          buildAddInputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildInput)
        }
        val startBuildGetOp = System.nanoTime()
        try {
          op.getOutput
        } catch {
          case e: Exception => {
            if (isShared) {
              OmniNestedLoopJoinBuildOperatorFactory.dereferenceNestedBuilderOperatorAndFactory(buildPlanId)
            } else {
              op.close()
              opFactory.close()
            }
            throw new RuntimeException("Nested loop join builder getOutput failed")
          }
        }
        buildGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildGetOp)
        (opFactory, op)
      }
      var buildOp: OmniOperator = null
      var buildOpFactory: OmniNestedLoopJoinBuildOperatorFactory = null
      if (enableShareBuildOp) {
        OmniNestedLoopJoinBuildOperatorFactory.gLock.lock()
        try {
          buildOpFactory = OmniNestedLoopJoinBuildOperatorFactory.getNestedLoopJoinBuilderOperatorFactory(buildPlanId)
          if (buildOpFactory == null) {
            val (opFactory, op) = createBuildOpFactoryAndOp(true)
            buildOpFactory = opFactory
            buildOp = op
          }
        } catch {
          case e: Exception => {
            throw new RuntimeException("nested loop build failed. errmsg:" + e.getMessage())
          }
        } finally {
          OmniNestedLoopJoinBuildOperatorFactory.gLock.unlock()
        }
      } else {
        val (opFactory, op) = createBuildOpFactoryAndOp(false)
        buildOpFactory = opFactory
        buildOp = op
      }


      val startLookupCodegen = System.nanoTime()
      val lookupOpFactory = new OmniNestedLoopJoinLookupOperatorFactory(lookupJoinType, probeTypes,probeOutputCols,filter,buildOpFactory,
        new OperatorConfig(SpillConfig.NONE,
          new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp = lookupOpFactory.createOperator()
      lookupCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        lookupOp.close()
        lookupOpFactory.close()
        if (enableShareBuildOp) {
          OmniNestedLoopJoinBuildOperatorFactory.gLock.lock()
          OmniNestedLoopJoinBuildOperatorFactory.dereferenceNestedBuilderOperatorAndFactory(buildPlanId)
          OmniNestedLoopJoinBuildOperatorFactory.gLock.unlock()
        } else {
          buildOp.close()
          buildOpFactory.close()
        }
      })

      val resultSchema = this.schema
      val reverse = buildSide == BuildLeft
      var left = 0
      var leftLen = prunedStreamedOutput.size
      var right = prunedStreamedOutput.size
      var rightLen = output.size
      if (reverse) {
        left = prunedStreamedOutput.size
        leftLen = output.size
        right = 0
        rightLen = prunedStreamedOutput.size
      }

      val iterBatch = new Iterator[ColumnarBatch] {
        private var results: java.util.Iterator[VecBatch] = _
        var res: Boolean = true

        override def hasNext: Boolean = {
          while ((results == null || !res) && iter.hasNext) {
            val batch = iter.next()
            val input = transColBatchToOmniVecs(batch)
            val vecBatch = new VecBatch(input, batch.numRows())
            val startlookupInput = System.nanoTime()
            lookupOp.addInput(vecBatch)
            lookupAddInputTime += NANOSECONDS.toMillis(System.nanoTime() - startlookupInput)
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
          if (projectList.nonEmpty) {
            reorderOutputVecs(projectListIndex, resultVecs, vecs)
          } else {
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
          }
          val rowCnt: Int = result.getRowCount
          numOutputRows += rowCnt
          numOutputVecBatches += 1
          result.close()
          new ColumnarBatch(vecs.toArray, rowCnt)
        }
      }

      if (enableJoinBatchMerge) {
        val mergeIterator = new MergeIterator(iterBatch, resultSchema, numMergedVecBatches)
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          mergeIterator.close()
        })
        mergeIterator
      } else {
        iterBatch
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException(s"This operator doesn't support doProduce().")
  }

  override def output: Seq[Attribute] = {
    if (projectList.nonEmpty) {
      projectList.map(_.toAttribute)
    } else {
      joinType match {
        case Inner =>
          left.output ++ right.output
        case LeftOuter =>
          left.output ++ right.output.map(_.withNullability(true))
        case RightOuter =>
          left.output.map(_.withNullability(true)) ++ right.output
        case j: ExistenceJoin =>
          left.output :+ j.exists
        case LeftExistence(_) =>
          left.output
        case x =>
          throw new IllegalArgumentException(s"NestedLoopJoin should not take $x as the JoinType")
      }
    }
  }
}
