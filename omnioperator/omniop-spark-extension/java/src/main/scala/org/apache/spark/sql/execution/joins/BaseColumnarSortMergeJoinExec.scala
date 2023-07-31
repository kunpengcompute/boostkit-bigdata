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

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, isSimpleColumn, isSimpleColumnForAll}
import nova.hetu.omniruntime.`type`.DataType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a sort merge join of two child relations.
 */
abstract class BaseColumnarSortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = Seq.empty)
  extends ShuffledJoin with CodegenSupport {

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = {
    if (isSkewJoin) "OmniColumnarSortMergeJoin(skew=true)" else "OmniColumnarSortMergeJoin"
  }

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      super.requiredChildDistribution
    }
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map { case (lKey, rKey) =>
        val sameOrderExpressions = ExpressionSet(lKey.sameOrderExpressions ++ rKey.children)
        SortOrder(lKey.child, Ascending, sameOrderExpressions.toSeq)
      }
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    case FullOuter => Nil
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private def getKeyOrdering(keys: Seq[Expression], childOutputOrdering: Seq[SortOrder])
    : Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map { case (key, childOrder) =>
        val sameOrderExpressionSet = ExpressionSet(childOrder.children) - key
        SortOrder(key, Ascending, sameOrderExpressionSet.toSeq)
      }
    } else {
      requiredOrdering
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    keys.map(SortOrder(_, Ascending))
  }

  override def output : Seq[Attribute] = {
    if (projectList.nonEmpty) {
      projectList.map(_.toAttribute)
    } else {
      super[ShuffledJoin].output
    }
  }

  override def needCopyResult: Boolean = true

  val SMJ_NEED_ADD_STREAM_TBL_DATA = 2
  val SMJ_NEED_ADD_BUFFERED_TBL_DATA = 3
  val SCAN_FINISH = 4

  val RES_INIT = 0
  val SMJ_FETCH_JOIN_DATA = 5

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "streamedAddInputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed addInput"),
    "streamedCodegenTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni streamed codegen"),
    "bufferedAddInputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered addInput"),
    "bufferedCodegenTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered codegen"),
    "getOutputTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "time in omni buffered getOutput"),
    "numOutputVecBatchs" ->
      SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    "numMergedVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatchs"),
    "numStreamVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of streamed vecBatchs"),
    "numBufferVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of buffered vecBatchs")
  )

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}${condition.get.dataType}"
    } else "None"

    s"""
       |$formattedNodeName
       |$simpleStringWithNodeId
       |${ExplainUtils.generateFieldString("Stream input", left.output ++ left.output.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Buffer input", right.output ++ right.output.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Left keys", leftKeys ++ leftKeys.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Right keys", rightKeys ++ rightKeys.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
       |${ExplainUtils.generateFieldString("Project List", projectList ++ projectList.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Output", output ++ output.map(_.dataType))}
       |Condition : $condition
       |""".stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
     throw new UnsupportedOperationException(s"This operator doesn't support doExecute.")
  }

  protected override def doProduce(ctx: CodegenContext): String = {
     throw new UnsupportedOperationException(s"This operator doesn't support doProduce.")
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    left.execute() :: right.execute() :: Nil
  }

  def buildCheck(): Unit = {
    joinType match {
      case Inner | LeftOuter | FullOuter | LeftSemi | LeftAnti =>
      // SMJ join support Inner | LeftOuter | FullOuter | LeftSemi | LeftAnti
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
          s"in ${this.nodeName}")
    }

    val streamedTypes = new Array[DataType](left.output.size)
    left.output.zipWithIndex.foreach { case (attr, i) =>
      streamedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val streamedKeyColsExp: Array[AnyRef] = leftKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(left.output.map(_.toAttribute)))
    }.toArray

    val bufferedTypes = new Array[DataType](right.output.size)
    right.output.zipWithIndex.foreach { case (attr, i) =>
      bufferedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val bufferedKeyColsExp: Array[AnyRef] = rightKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(right.output.map(_.toAttribute)))
    }.toArray

    if (!isSimpleColumnForAll(streamedKeyColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", streamedKeyColsExp)
    }

    if (!isSimpleColumnForAll(bufferedKeyColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", bufferedKeyColsExp)
    }

    condition match {
      case Some(expr) =>
        val filterExpr: String = OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap((left.output ++ right.output).map(_.toAttribute)))
        if (!isSimpleColumn(filterExpr)) {
          checkOmniJsonWhiteList(filterExpr, new Array[AnyRef](0))
        }
      case _ => null
    }
  }
}