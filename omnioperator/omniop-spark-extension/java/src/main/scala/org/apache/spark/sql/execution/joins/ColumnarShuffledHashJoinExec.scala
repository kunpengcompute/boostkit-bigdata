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

import java.util.Optional
import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, isSimpleColumn, isSimpleColumnForAll}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{getIndexArray, pruneOutput, reorderVecs, transColBatchToOmniVecs}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniHashBuilderWithExprOperatorFactory, OmniLookupJoinWithExprOperatorFactory, OmniLookupOuterJoinWithExprOperatorFactory}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildSide}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
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
    right: SparkPlan,
    isSkewJoin: Boolean,
    projectList: Seq[NamedExpression] = Seq.empty)
  extends HashJoin with ShuffledJoin {

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}${condition.get.dataType}"
    } else "None"
    s"""
       |$formattedNodeName
       |$simpleStringWithNodeId
       |${ExplainUtils.generateFieldString("buildOutput", buildOutput ++ buildOutput.map(_.dataType))}
       |${ExplainUtils.generateFieldString("streamedOutput", streamedOutput ++ streamedOutput.map(_.dataType))}
       |${ExplainUtils.generateFieldString("leftKeys", leftKeys ++ leftKeys.map(_.dataType))}
       |${ExplainUtils.generateFieldString("rightKeys", rightKeys ++ rightKeys.map(_.dataType))}
       |${ExplainUtils.generateFieldString("condition", joinCondStr)}
       |${ExplainUtils.generateFieldString("projectList", projectList.map(_.toAttribute) ++ projectList.map(_.toAttribute).map(_.dataType))}
       |${ExplainUtils.generateFieldString("output", output ++ output.map(_.dataType))}
       |Condition : $condition
       |""".stripMargin
  }

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

  override def output: Seq[Attribute] = {
    if (projectList.nonEmpty) {
      projectList.map(_.toAttribute)
    } else {
      super[ShuffledJoin].output
     }
  }

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan):
    ColumnarShuffledHashJoinExec = copy(left = newLeft, right = newRight)

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil
    case _ => super.outputOrdering
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"Should not be called when supports columnar execution")
  }

  def buildCheck(): Unit = {
    joinType match {
      case FullOuter | Inner | LeftAnti | LeftOuter | LeftSemi =>
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
        s"in ${this.nodeName}")
    }
    val buildTypes = new Array[DataType](buildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val buildJoinColsExp = buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray

    if (!isSimpleColumnForAll(buildJoinColsExp)) {
      checkOmniJsonWhiteList("", buildJoinColsExp.asInstanceOf[Array[AnyRef]])
    }

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    val probeHashColsExp = streamedKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(streamedOutput.map(_.toAttribute)))
    }.toArray

    if (!isSimpleColumnForAll(probeHashColsExp)) {
      checkOmniJsonWhiteList("", probeHashColsExp.asInstanceOf[Array[AnyRef]])
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

    val buildTypes = new Array[DataType](buildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val buildOutputCols: Array[Int] = joinType match {
      case Inner | FullOuter | LeftOuter =>
        getIndexArray(buildOutput, projectList)
      case LeftExistence(_) =>
        Array[Int]()
      case x =>
        throw new UnsupportedOperationException(s"ColumnShuffledHashJoin Join-type[$x] is not supported!")
    }

    val buildJoinColsExp = buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray

    val prunedBuildOutput = pruneOutput(buildOutput, projectList)
    val buildOutputTypes = new Array[DataType](prunedBuildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    prunedBuildOutput.zipWithIndex.foreach { case (att, i) =>
      buildOutputTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = getIndexArray(streamedOutput, projectList)
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
          buildJoinColsExp, filter, 1, new OperatorConfig(SpillConfig.NONE,
            new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        val buildOp = buildOpFactory.createOperator()
        buildCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildCodegen)

        val startLookupCodegen = System.nanoTime()
        val lookupJoinType = OmniExpressionAdaptor.toOmniJoinType(joinType)
        val lookupOpFactory = new OmniLookupJoinWithExprOperatorFactory(probeTypes,
          probeOutputCols, probeHashColsExp, buildOutputCols, buildOutputTypes, lookupJoinType,
          buildOpFactory, new OperatorConfig(SpillConfig.NONE,
            new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        val lookupOp = lookupOpFactory.createOperator()
        lookupCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupCodegen)

        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          lookupOp.close()
          buildOp.close()
          lookupOpFactory.close()
          buildOpFactory.close()
        })

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

        val streamedPlanOutput = pruneOutput(streamedPlan.output, projectList)
        val prunedOutput = streamedPlanOutput ++ prunedBuildOutput
        val resultSchema = this.schema
        val reverse = buildSide == BuildLeft
        var left = 0
        var leftLen = streamedPlanOutput.size
        var right = streamedPlanOutput.size
        var rightLen = output.size
        if (reverse) {
          left = streamedPlanOutput.size
          leftLen = output.size
          right = 0
          rightLen = streamedPlanOutput.size
        }

        val joinIter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
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
            if (projectList.nonEmpty) {
              reorderVecs(prunedOutput, projectList, resultVecs, vecs)
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
            numOutputVecBatchs += 1
            result.close()
            new ColumnarBatch(vecs.toArray, rowCnt)
          }
        }
        if ("FULL OUTER" == joinType.sql) {
          val lookupOuterOpFactory =
            new OmniLookupOuterJoinWithExprOperatorFactory(probeTypes, probeOutputCols,
              probeHashColsExp, buildOutputCols, buildOutputTypes, buildOpFactory,
              new OperatorConfig(SpillConfig.NONE,
                new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))

          val lookupOuterOp = lookupOuterOpFactory.createOperator()
          lookupCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupCodegen)
          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            lookupOuterOp.close()
            lookupOuterOpFactory.close()
          })

          val appendIter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
            var output: java.util.Iterator[VecBatch] = _

            override def hasNext: Boolean = {
              if (output == null) {
                output = lookupOuterOp.getOutput
              }
              output.hasNext
            }

            override def next(): ColumnarBatch = {
              val result = output.next()
              val resultVecs = result.getVectors
              val vecs = OmniColumnVector
                .allocateColumns(result.getRowCount, resultSchema, false)
              if (projectList.nonEmpty) {
                reorderVecs(prunedOutput, projectList, resultVecs, vecs)
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
              numOutputRows += result.getRowCount
              numOutputVecBatchs += 1
              new ColumnarBatch(vecs.toArray, result.getRowCount)

            }
          }
          joinIter ++ appendIter
        } else {
          joinIter
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
