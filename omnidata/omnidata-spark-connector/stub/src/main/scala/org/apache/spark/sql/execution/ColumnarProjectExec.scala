package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}

case class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
    with AliasAwareOutputPartitioning
    with AliasAwareOutputOrdering {
  override protected def orderingExpressions: Seq[SortOrder] = ???

  override protected def outputExpressions: Seq[NamedExpression] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] = ???
}
