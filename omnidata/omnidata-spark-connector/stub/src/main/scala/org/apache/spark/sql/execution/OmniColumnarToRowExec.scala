package org.apache.spark.sql.execution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

case class OmniColumnarToRowExec(child: SparkPlan) extends ColumnarToRowTransition {
  override protected def doExecute(): RDD[InternalRow] = ???
  override def output: Seq[Attribute] = ???
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???
}
