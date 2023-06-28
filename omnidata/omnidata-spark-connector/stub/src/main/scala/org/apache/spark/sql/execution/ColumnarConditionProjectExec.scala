package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}

case class ColumnarConditionProjectExec(projectList: Seq[NamedExpression],
                                        condition: Expression,
                                        child: SparkPlan)
  extends UnaryExecNode
    with AliasAwareOutputPartitioning
    with AliasAwareOutputOrdering {
  override protected def orderingExpressions: Seq[SortOrder] = ???

  override protected def outputExpressions: Seq[NamedExpression] = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???

  override def output: Seq[Attribute] = ???
}
