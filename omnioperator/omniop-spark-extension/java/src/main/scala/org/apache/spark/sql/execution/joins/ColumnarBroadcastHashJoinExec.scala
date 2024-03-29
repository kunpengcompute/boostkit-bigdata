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
import scala.collection.mutable
import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, isSimpleColumn, isSimpleColumnForAll}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{getIndexArray, pruneOutput, reorderVecs, transColBatchToOmniVecs}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.OmniOperator
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniHashBuilderWithExprOperatorFactory, OmniLookupJoinWithExprOperatorFactory}
import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarHashedRelation, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.{MergeIterator, SparkMemoryUtils}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class ColumnarBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean = false,
    projectList: Seq[NamedExpression] = Seq.empty)
  extends HashJoin {

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

  if (isNullAwareAntiJoin) {
    require(leftKeys.length == 1, "leftKeys length should be 1")
    require(rightKeys.length == 1, "rightKeys length should be 1")
    require(joinType == LeftAnti, "joinType must be LeftAnti.")
    require(buildSide == BuildRight, "buildSide must be BuildRight.")
    require(condition.isEmpty, "null aware anti join optimize condition should be empty.")
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
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    "numMergedVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatchs")
  )

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = "OmniColumnarBroadcastHashJoin"

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override lazy val outputPartitioning: Partitioning = {
    joinType match {
      case Inner if sqlContext.conf.broadcastHashJoinOutputPartitioningExpandLimit > 0 =>
        streamedPlan.outputPartitioning match {
          case h: HashPartitioning => expandOutputPartitioning(h)
          case c: PartitioningCollection => expandOutputPartitioning(c)
          case other => other
        }
      case _ => streamedPlan.outputPartitioning
    }
  }

  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeys.zip(buildKeys).foreach {
      case (streamedKey, buildKey) =>
        val key = streamedKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ buildKey)
          case None => mapping.put(key, Seq(buildKey))
        }
    }
    mapping.toMap
  }

  // Expands the given partitioning collection recursively.
  private def expandOutputPartitioning(partitioning: PartitioningCollection)
  : PartitioningCollection = {
    PartitioningCollection(partitioning.partitionings.flatMap {
      case h: HashPartitioning => expandOutputPartitioning(h).partitionings
      case c: PartitioningCollection => Seq(expandOutputPartitioning(c))
      case other => Seq(other)
    })
  }

  // Expands the given hash partitioning by substituting streamed keys with build keys.
  // For example, if the expressions for the given partitioning are Seq("a", "b", "c")
  // where the streamed keys are Seq("b", "c") and the build keys are Seq("x", "y"),
  // the expanded partitioning will have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  // The expanded expressions are returned as PartitioningCollection.
  private def expandOutputPartitioning(partitioning: HashPartitioning): PartitioningCollection = {
    val maxNumCombinations = sqlContext.conf.broadcastHashJoinOutputPartitioningExpandLimit
    var currentNumCombinations = 0

    def generateExprCombinations(
                                  current: Seq[Expression],
                                  accumulated: Seq[Expression]): Seq[Seq[Expression]] = {
      if (currentNumCombinations >= maxNumCombinations) {
        Nil
      } else if (current.isEmpty) {
        currentNumCombinations += 1
        Seq(accumulated)
      } else {
        val buildKeysOpt = streamedKeyToBuildKeyMapping.get(current.head.canonicalized)
        generateExprCombinations(current.tail, accumulated :+ current.head) ++
          buildKeysOpt.map(_.flatMap(b => generateExprCombinations(current.tail, accumulated :+ b)))
            .getOrElse(Nil)
      }
    }

    PartitioningCollection(
      generateExprCombinations(partitioning.expressions, Nil)
        .map(HashPartitioning(_, partitioning.numPartitions)))
  }

  /** only for operator fusion */
  def getBuildOutput: Seq[Attribute] = {
    buildOutput
  }

  def getBuildKeys: Seq[Expression] = {
    buildKeys
  }

  def getBuildPlan: SparkPlan = {
    buildPlan
  }

  def getStreamedOutput: Seq[Attribute] = {
    streamedOutput
  }

  def getStreamedKeys: Seq[Expression] = {
    streamedKeys
  }

  def getStreamPlan: SparkPlan = {
    streamedPlan
  }

  def buildCheck(): Unit = {
    joinType match {
      case LeftOuter | Inner | LeftSemi =>
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
          s"in ${this.nodeName}")
    }

    val buildTypes = new Array[DataType](buildOutput.size) // {2, 2}, buildOutput:col1#12,col2#13
    buildOutput.zipWithIndex.foreach {case (att, i) =>
    buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val buildJoinColsExp: Array[AnyRef] = buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray

    val probeTypes = new Array[DataType](streamedOutput.size)
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeHashColsExp: Array[AnyRef] = streamedKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(streamedOutput.map(_.toAttribute)))
    }.toArray

    if (!isSimpleColumnForAll(buildJoinColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", buildJoinColsExp)
    }
    if (!isSimpleColumnForAll(probeHashColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", probeHashColsExp)
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
   * Return true if this stage of the plan supports columnar execution.
   */

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // input/output: {col1#10,col2#11,col1#12,col2#13}
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val numMergedVecBatchs = longMetric("numMergedVecBatchs")
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
    val enableShareBuildOp: Boolean = columnarConf.enableShareBroadcastJoinHashTable
    val enableJoinBatchMerge: Boolean = columnarConf.enableJoinBatchMerge

    var canShareBuildOp: Boolean = false
    // {0}, buildKeys: col1#12
    val buildOutputCols: Array[Int] = joinType match {
      case Inner | LeftOuter =>
        canShareBuildOp = true
        getIndexArray(buildOutput, projectList)
      case LeftExistence(_) =>
        canShareBuildOp = false
        Array[Int]()
      case x =>
        throw new UnsupportedOperationException(s"ColumnBroadcastHashJoin Join-type[$x] is not supported!")
    }
    val buildJoinColsExp = buildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(buildOutput.map(_.toAttribute)))
    }.toArray
    val relation = buildPlan.executeBroadcast[ColumnarHashedRelation]()

    val prunedBuildOutput = pruneOutput(buildOutput, projectList)
    val buildOutputTypes = new Array[DataType](prunedBuildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    prunedBuildOutput.zipWithIndex.foreach { case (att, i) =>
      buildOutputTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    val probeTypes = new Array[DataType](streamedOutput.size) // {2,2}, streamedOutput:col1#10,col2#11
    streamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = getIndexArray(streamedOutput, projectList) // {0,1}
    val probeHashColsExp = streamedKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(streamedOutput.map(_.toAttribute)))
    }.toArray
    streamedPlan.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val filter: Optional[String] = condition match {
        case Some(expr) =>
          canShareBuildOp = false
          Optional.of(OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
            OmniExpressionAdaptor.getExprIdMap((streamedOutput ++ buildOutput).map(_.toAttribute))))
        case _ =>
          canShareBuildOp = true
          Optional.empty()
      }

      def createBuildOpFactoryAndOp(): (OmniHashBuilderWithExprOperatorFactory, OmniOperator) = {
        val startBuildCodegen = System.nanoTime()
        val opFactory =
          new OmniHashBuilderWithExprOperatorFactory(buildTypes, buildJoinColsExp, filter, 1,
           new OperatorConfig(SpillConfig.NONE,
             new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        val op = opFactory.createOperator()
        buildCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildCodegen)

        val deserializer = VecBatchSerializerFactory.create()
        relation.value.buildData.foreach { input =>
          val startBuildInput = System.nanoTime()
          op.addInput(deserializer.deserialize(input))
          buildAddInputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildInput)
        }
        val startBuildGetOp = System.nanoTime()
        op.getOutput
        buildGetOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startBuildGetOp)
        (opFactory, op)
      }

      var buildOp: OmniOperator = null
      var buildOpFactory: OmniHashBuilderWithExprOperatorFactory = null
      if (enableShareBuildOp && canShareBuildOp) {
        OmniHashBuilderWithExprOperatorFactory.gLock.lock()
        try {
          buildOpFactory = OmniHashBuilderWithExprOperatorFactory.getHashBuilderOperatorFactory(buildPlan.id)
          if (buildOpFactory == null) {
            val (opFactory, op) = createBuildOpFactoryAndOp()
            buildOpFactory = opFactory
            buildOp = op
            OmniHashBuilderWithExprOperatorFactory.saveHashBuilderOperatorAndFactory(buildPlan.id,
              buildOpFactory, buildOp)
          }
        } catch {
          case e: Exception => {
            throw new RuntimeException("hash build failed. errmsg:" + e.getMessage())
          }
        } finally {
          OmniHashBuilderWithExprOperatorFactory.gLock.unlock()
        }
      } else {
        val (opFactory, op) = createBuildOpFactoryAndOp()
        buildOpFactory = opFactory
        buildOp = op
      }

      val startLookupCodegen = System.nanoTime()
      val lookupJoinType = OmniExpressionAdaptor.toOmniJoinType(joinType)
      val lookupOpFactory = new OmniLookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols,
        probeHashColsExp, buildOutputCols, buildOutputTypes, lookupJoinType, buildOpFactory,
        new OperatorConfig(SpillConfig.NONE,
          new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp = lookupOpFactory.createOperator()
      lookupCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startLookupCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        lookupOp.close()
        lookupOpFactory.close()
        if (enableShareBuildOp && canShareBuildOp) {
          OmniHashBuilderWithExprOperatorFactory.gLock.lock()
          OmniHashBuilderWithExprOperatorFactory.dereferenceHashBuilderOperatorAndFactory(buildPlan.id)
          OmniHashBuilderWithExprOperatorFactory.gLock.unlock()
        } else {
          buildOp.close()
          buildOpFactory.close()
        }
      })

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

      if (enableJoinBatchMerge) {
        new MergeIterator(iterBatch, resultSchema, numMergedVecBatchs)
      } else {
        iterBatch
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  private def multipleOutputForOneInput: Boolean = joinType match {
    case Inner | LeftOuter | RightOuter =>
      // For inner and outer joins, one row from the streamed side may produce multiple result rows,
      // if the build side has duplicated keys. Note that here we wait for the broadcast to be
      // finished, which is a no-op because it's already finished when we wait it in `doProduce`.
      !buildPlan.executeBroadcast[HashedRelation]().value.keyIsUnique

    // Other joins types(semi, anti, existence) can at most produce one result row for one input
    // row from the streamed side.
    case _ => false
  }

  // If the streaming side needs to copy result, this join plan needs to copy too. Otherwise,
  // this join plan only needs to copy result if it may output multiple rows for one input.
  override def needCopyResult: Boolean =
    streamedPlan.asInstanceOf[CodegenSupport].needCopyResult || multipleOutputForOneInput

  /**
   * Returns a tuple of Broadcast of HashedRelation and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Broadcast[HashedRelation], String) = {
    throw new UnsupportedOperationException(s"This operator doesn't support prepareBroadcast().")
  }

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    throw new UnsupportedOperationException(s"This operator doesn't support prepareRelation().")
  }

  protected override def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    throw new UnsupportedOperationException(s"This operator doesn't support codegenAnti().")
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
          throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
      }
    }
  }


}
