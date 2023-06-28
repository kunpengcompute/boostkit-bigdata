package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}

case class ColumnarFilterExec(condition: Expression, child: SparkPlan)
    extends UnaryExecNode with PredicateHelper {
  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???
}
