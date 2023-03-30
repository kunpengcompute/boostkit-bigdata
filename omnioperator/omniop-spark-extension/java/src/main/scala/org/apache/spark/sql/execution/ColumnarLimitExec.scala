package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarBaseLimitExec extends LimitExec {

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val hasInput = iter.hasNext
      if (hasInput) {
        new Iterator[ColumnarBatch] {
          var rowCount = 0
          override def hasNext: Boolean = {
            val hasNext = iter.hasNext
            hasNext && (rowCount < limit)
          }

          override def next(): ColumnarBatch = {
            val output = iter.next()
            val preRowCount = rowCount
            rowCount += output.numRows
            if (rowCount > limit) {
              val newSize = limit - preRowCount
              output.setNumRows(newSize)
            }
            output
          }
        }
      } else {
        Iterator.empty
      }
    }
  }

  protected override def doExecute() = {
    throw new UnsupportedOperationException("This operator doesn't support doExecute()")
  }
}

case class ColumnarLocalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def nodeName: String = "OmniColumnarLocalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class ColumnarGlobalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def nodeName: String = "OmniColumnarGlobalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
