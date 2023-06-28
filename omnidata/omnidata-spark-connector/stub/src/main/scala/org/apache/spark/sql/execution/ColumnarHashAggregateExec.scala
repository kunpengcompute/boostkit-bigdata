package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

case class ColumnarHashAggregateExec(
                                      requiredChildDistributionExpressions: Option[Seq[Expression]],
                                      groupingExpressions: Seq[NamedExpression],
                                      aggregateExpressions: Seq[AggregateExpression],
                                      aggregateAttributes: Seq[Attribute],
                                      initialInputBufferOffset: Int,
                                      resultExpressions: Seq[NamedExpression],
                                      child: SparkPlan)
  extends BaseAggregateExec
    with AliasAwareOutputPartitioning {
  override protected def doExecute(): RDD[InternalRow] = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???
}
