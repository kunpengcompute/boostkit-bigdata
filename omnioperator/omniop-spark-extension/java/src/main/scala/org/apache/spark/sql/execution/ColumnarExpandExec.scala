package org.apache.spark.sql.execution

import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, getExprIdMap, rewriteToOmniJsonExpressionLiteral, sparkTypeToOmniType}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.operator.config.OperatorConfig
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.vector.{LongVec, Vec, VecBatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.concurrent.duration.NANOSECONDS

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 *
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param child       Child operator
 */
case class ColumnarExpandExec(
                               projections: Seq[Seq[Expression]],
                               output: Seq[Attribute],
                               child: SparkPlan)
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarExpand"

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
  )

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    child.output.foreach(exp => sparkTypeToOmniType(exp.dataType, exp.metadata))
    val omniExpressions: Array[Array[AnyRef]] = projections.map(exps => exps.map(
      exp => {
        val omniExp: AnyRef = rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)
        omniExp
      }
    ).toArray).toArray
    omniExpressions.foreach(exps => checkOmniJsonWhiteList("", exps))
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRowsMetric = longMetric("numOutputRows")
    val numOutputVecBatchsMetric = longMetric("numOutputVecBatchs")
    val addInputTimeMetric = longMetric("addInputTime")
    val omniCodegenTimeMetric = longMetric("omniCodegenTime")
    val getOutputTimeMetric = longMetric("getOutputTime")

    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = projections.map(exps => exps.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)
    ).toArray).toArray

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      var projectOperators = omniExpressions.map(exps => {
        val factory = new OmniProjectOperatorFactory(exps, omniInputTypes, 1, new OperatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP))
        factory.createOperator
      })
      omniCodegenTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperators.foreach(operator => operator.close())
      })

      new Iterator[ColumnarBatch] {
        private var results: java.util.Iterator[VecBatch] = _
        private[this] var idx = -1 // -1 mean the initial state
        private[this] var input: ColumnarBatch = _

        def isInputHasNext: Boolean = {
          (-1 < idx && idx < projections.length) || iter.hasNext
        }

        override def hasNext: Boolean = {
          while ((results == null || !results.hasNext) && isInputHasNext) {
            if (idx <= 0) {
              // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
              input = iter.next()
              idx = 0
            }
            // last group should not copy input, and let omnioperator free input vec
            val isSlice = idx < projections.length - 1
            val omniInput = transColBatchToOmniVecs(input, isSlice)
            val vecBatch = new VecBatch(omniInput, input.numRows())

            val startInput = System.nanoTime()
            projectOperators(idx).addInput(vecBatch)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)

            val startGetOutput = System.nanoTime()
            results = projectOperators(idx).getOutput
            getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

            idx += 1
            if (idx == projections.length && iter.hasNext) {
              idx = 0
            }
          }

          if (results == null) {
            return false
          }
          val startGetOutput = System.nanoTime()
          val hasNext = results.hasNext
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startGetOutput = System.nanoTime()
          val result = results.next()
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            result.getRowCount, schema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(result.getVectors()(i))
          }

          val rowCount = result.getRowCount
          numOutputRowsMetric += rowCount
          numOutputVecBatchsMetric += 1
          result.close()
          new ColumnarBatch(vectors.toArray, rowCount)
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarExpandExec operator doesn't support doExecute().")
  }

}
