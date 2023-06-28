package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

case class ColumnarSortMergeJoinExec(
                                      leftKeys: Seq[Expression],
                                      rightKeys: Seq[Expression],
                                      joinType: JoinType,
                                      condition: Option[Expression],
                                      left: SparkPlan,
                                      right: SparkPlan,
                                      isSkewJoin: Boolean = false,
                                      projectList: Seq[NamedExpression] = Seq.empty)
  extends ShuffledJoin with CodegenSupport {
  override def inputRDDs(): Seq[RDD[InternalRow]] = ???

  override protected def doProduce(ctx: CodegenContext): String = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = ???
}
