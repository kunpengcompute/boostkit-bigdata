package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan

case class ColumnarShuffledHashJoinExec(
                                         leftKeys: Seq[Expression],
                                         rightKeys: Seq[Expression],
                                         joinType: JoinType,
                                         buildSide: BuildSide,
                                         condition: Option[Expression],
                                         left: SparkPlan,
                                         right: SparkPlan,
                                         projectList: Seq[NamedExpression] = Seq.empty)
  extends HashJoin with ShuffledJoin {
  override protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo = ???

  override def isSkewJoin: Boolean = ???

  override def inputRDDs(): Seq[RDD[InternalRow]] = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = ???
}
