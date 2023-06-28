package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}

case class ColumnarSortExec(sortOrder: Seq[SortOrder],
                            global: Boolean,
                            child: SparkPlan,
                            testSpillFrequency: Int = 0)
  extends UnaryExecNode{
  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???
}
