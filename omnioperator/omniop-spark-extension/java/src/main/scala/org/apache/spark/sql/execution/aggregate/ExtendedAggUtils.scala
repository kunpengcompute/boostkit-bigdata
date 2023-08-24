package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Partial}
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.execution.SparkPlan

object ExtendedAggUtils {
  def normalizeGroupingExpressions(groupingExpressions: Seq[NamedExpression]) = {
    groupingExpressions.map { e =>
      NormalizeFloatingNumbers.normalize(e) match {
        case n: NamedExpression => n
        case other => Alias(other, e.name)(exprId = e.exprId)
      }
    }
  }

  def planPartialAggregateWithoutDistinct(
                                           groupingExpressions: Seq[NamedExpression],
                                           aggregateExpressions: Seq[AggregateExpression],
                                           resultExpressions: Seq[NamedExpression],
                                           child: SparkPlan): SparkPlan = {
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    createAggregate(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions.map(_.toAttribute),
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
      initialInputBufferOffset = groupingExpressions.length,
      resultExpressions = resultExpressions,
      child = child)
  }

  private def createAggregate(
                               requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
                               isStreaming: Boolean = false,
                               groupingExpressions: Seq[NamedExpression] = Nil,
                               aggregateExpressions: Seq[AggregateExpression] = Nil,
                               aggregateAttributes: Seq[Attribute] = Nil,
                               initialInputBufferOffset: Int = 0,
                               resultExpressions: Seq[NamedExpression] = Nil,
                               child: SparkPlan): SparkPlan = {
    val useHash = HashAggregateExec.supportsAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      val objectHashEnabled = child.sqlContext.conf.useObjectHashAggregation
      val useObjectHash = ObjectHashAggregateExec.supportsAggregate(aggregateExpressions)

      if (objectHashEnabled && useObjectHash) {
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      } else {
        SortAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      }
    }
  }

  private def mayRemoveAggFilters(exprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    exprs.map { ae =>
      if (ae.filter.isDefined) {
        ae.mode match {
          case Partial | Complete => ae
          case _ => ae.copy(filter = None)
        }
      } else {
        ae
      }
    }
  }
}

case class DummyLogicalPlan() extends LeafNode {
  override def output: Seq[Attribute] = Nil

  override def computeStats(): Statistics = throw new UnsupportedOperationException
}