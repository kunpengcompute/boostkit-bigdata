/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import com.huawei.boostkit.spark.ColumnarPluginConfig

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, Filter, Join, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.ShimUtil

/**
 * The skeleton of this rule is just as same as MergeScalarSubqueries Rule. This rule relaxes the
 * constraint of filters which can be merged.
 */
object MergeSubqueryFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Subquery reuse needs to be enabled for this optimization.
      case _ if !conf.getConf(SQLConf.SUBQUERY_REUSE_ENABLED) => plan

      // This rule does a whole plan traversal, no need to run on subqueries.
      case _: Subquery => plan

      // Plans with CTEs are not supported for now.
      case _: WithCTE => plan

      case _ => extractCommonScalarSubqueries(plan)
    }
  }

  /**
   * An item in the cache of merged scalar subqueries.
   *
   * @param attributes Attributes that form the struct scalar return value of a merged subquery.
   * @param plan       The plan of a merged scalar subquery.
   * @param merged     A flag to identify if this item is the result of merging subqueries.
   *                   Please note that `attributes.size == 1` doesn't always mean that the plan
   *                   is not merged as there can be subqueries that are different
   *                   ([[checkIdenticalPlans]] is false) due to an extra [[Project]] node in
   *                   one of them. In that case `attributes.size` remains 1 after merging, but
   *                   the merged flag becomes true.
   * @param references A set of subquery indexes in the cache to track all (including transitive)
   *                   nested subqueries.
   */
  case class Header(
      attributes: Seq[Attribute],
      plan: LogicalPlan,
      merged: Boolean,
      references: Set[Int])

  private def extractCommonScalarSubqueries(plan: LogicalPlan) = {
    val cache = ArrayBuffer.empty[Header]
    val planWithReferences = insertReferences(plan, cache)
    cache.zipWithIndex.foreach { case (header, i) =>
      cache(i) = cache(i).copy(plan =
        if (header.merged) {
          ShimUtil.createCTERelationDef(
            createProject(header.attributes,
              removeReferences(removePropagatedFilters(header.plan), cache)),
            underSubquery = true)
        } else {
          removeReferences(header.plan, cache)
        })
    }
    val newPlan = removeReferences(planWithReferences, cache)
    val subqueryCTEs = cache.filter(_.merged).map(_.plan.asInstanceOf[CTERelationDef])
    if (subqueryCTEs.nonEmpty) {
      WithCTE(newPlan, subqueryCTEs.toSeq)
    } else {
      newPlan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(plan: LogicalPlan, cache: ArrayBuffer[Header]): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case n => n.transformExpressionsUpWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
        // The subquery could contain a hint that is not propagated once we cache it, but as a
        // non-correlated scalar subquery won't be turned into a Join the loss of hints is fine.
        case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
          val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
          ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
      }
    }
  }

  // Caching returns the index of the subquery in the cache and the index of scalar member in the
  // "Header".
  private def cacheSubquery(plan: LogicalPlan, cache: ArrayBuffer[Header]): (Int, Int) = {
    val output = plan.output.head
    val references = mutable.HashSet.empty[Int]
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
      case ssr: ScalarSubqueryReference =>
        references += ssr.subqueryIndex
        references ++= cache(ssr.subqueryIndex).references
        ssr
    }

    cache.zipWithIndex.collectFirst(Function.unlift {
      case (header, subqueryIndex) if !references.contains(subqueryIndex) =>
        checkIdenticalPlans(plan, header.plan).map { outputMap =>
          val mappedOutput = mapAttributes(output, outputMap)
          val headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
          subqueryIndex -> headerIndex
        }.orElse {
          tryMergePlans(plan, header.plan, false).collect {
            case (mergedPlan, outputMap, None, None, _) =>
              val mappedOutput = mapAttributes(output, outputMap)
              var headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
              val newHeaderAttributes = if (headerIndex == -1) {
                headerIndex = header.attributes.size
                header.attributes :+ mappedOutput
              } else {
                header.attributes
              }
              cache(subqueryIndex) =
                Header(newHeaderAttributes, mergedPlan, true, header.references ++ references)
              subqueryIndex -> headerIndex
          }
        }
      case _ => None
    }).getOrElse {
      cache += Header(Seq(output), plan, false, references.toSet)
      cache.length - 1 -> 0
    }
  }

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  private def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  /**
   * Recursively traverse down and try merging 2 plans.
   *
   * Please note that merging arbitrary plans can be complicated, the current version supports only
   * some of the most important nodes.
   *
   * @param newPlan                    a new plan that we want to merge to an already processed plan
   * @param cachedPlan                 a plan that we already processed, it can be either an
   *                                   original plan or a merged version of 2 or more plans
   * @param filterPropagationSupported a boolean flag that we propagate down to signal we have seen
   *                                   an `Aggregate` node where propagated filters can be merged
   * @return A tuple of:
   *         - the merged plan,
   *         - the attribute mapping from the new to the merged version,
   *         - the 2 optional filters of both plans that we need to propagate up and merge in
   *           an ancestor `Aggregate` node if possible,
   *         - the optional accumulated extra cost of merge that we need to propagate up and
   *           check in the ancestor `Aggregate` node.
   *           The cost is optional to signal if the cost needs to be taken into account up in the
   *           `Aggregate` node to decide about merge.
   */
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan,
      filterPropagationSupported: Boolean):
    Option[(LogicalPlan, AttributeMap[Attribute], Option[Expression], Option[Expression],
        Option[Double])] = {
    checkIdenticalPlans(newPlan, cachedPlan).map { outputMap =>
      // Currently the cost is always propagated up when `filterPropagationSupported` is true but
      // later we can address cases when we don't need to take cost into account. Please find the
      // details at the `Filter` node handling.
      val mergeCost = if (filterPropagationSupported) Some(0d) else None

      (cachedPlan, outputMap, None, None, mergeCost)
    }.orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child, filterPropagationSupported).map {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val (mergedProjectList, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost) =
                mergeNamedExpressions(np.projectList, outputMap, cp.projectList, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child, filterPropagationSupported).map {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val (mergedProjectList, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost) =
                mergeNamedExpressions(np.output, outputMap, cp.projectList, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp, filterPropagationSupported).map {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val (mergedProjectList, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost) =
                mergeNamedExpressions(np.projectList, outputMap, cp.output, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan = Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          val filterPropagationSupported =
            ColumnarPluginConfig.getConf.filterMergeEnable &&
              supportsFilterPropagation(np) && supportsFilterPropagation(cp)
          tryMergePlans(np.child, cp.child, filterPropagationSupported).flatMap {
            case (mergedChild, outputMap, None, None, _) =>
              val mappedNewGroupingExpression =
                np.groupingExpressions.map(mapAttributes(_, outputMap))
              // Order of grouping expression does matter as merging different grouping orders can
              // introduce "extra" shuffles/sorts that might not present in all of the original
              // subqueries.
              if (mappedNewGroupingExpression.map(_.canonicalized) ==
                cp.groupingExpressions.map(_.canonicalized)) {
                // No need to calculate and check costs as there is no propagated filter
                val (mergedAggregateExpressions, newOutputMap, _, _, _) =
                  mergeNamedExpressions(np.aggregateExpressions, outputMap, cp.aggregateExpressions,
                    None, None, None)
                val mergedPlan =
                  Aggregate(cp.groupingExpressions, mergedAggregateExpressions, mergedChild)
                Some(mergedPlan, newOutputMap, None, None, None)
              } else {
                None
              }
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              // No need to calculate cost in `mergeNamedExpressions()`
              val (mergedAggregateExpressions, newOutputMap, _, _, _) =
                mergeNamedExpressions(
                  filterAggregateExpressions(np.aggregateExpressions, newChildFilter),
                  outputMap,
                  filterAggregateExpressions(cp.aggregateExpressions, mergedChildFilter),
                  None,
                  None,
                  None)

              val mergeFilters = newChildFilter.isEmpty || mergedChildFilter.isEmpty || {
                val mergeCost = childMergeCost.map { c =>
                  val newPlanExtraCost = mergedChildFilter.map(getCost).getOrElse(0d) +
                    newChildFilter.map(getCost).getOrElse(0d)
                  val cachedPlanExtraCost = newPlanExtraCost
                  c + newPlanExtraCost + cachedPlanExtraCost
                }
                mergeCost.forall { c =>
                  val maxCost = ColumnarPluginConfig.getConf.filterMergeThreshold
                  val enableMerge = maxCost < 0 || c <= maxCost
                  if (!enableMerge) {
                    logDebug(
                      s"Plan merge of\n${np}and\n${cp}failed as the merge cost is too high: $c")
                  }
                  enableMerge
                }
              }
              if (mergeFilters) {
                val mergedPlan = Aggregate(Seq.empty, mergedAggregateExpressions, mergedChild)
                Some(mergedPlan, newOutputMap, None, None, None)
              } else {
                None
              }
            case _ => None
          }

        // Here is the difference with MergeScalarSubqueries Rule.
        // We can still merge the 'Filters' when they are not exactly the same.
        // The differing `Filter`s can be merged if:
        // - they both they have an ancestor `Aggregate` node that has no grouping and
        // - there are only `Project` or `Filter` nodes in between the different `Filters` and the
        //   ancestor `Aggregate` nodes.
        //
        // For example, we can merge:
        //
        // SELECT avg(a) FROM t WHERE c = 1
        //
        // and:
        //
        // SELECT sum(b) FROM t WHERE c = 2
        //
        // into:
        //
        // SELECT
        //   avg(a) FILTER (WHERE c = 1),
        //   sum(b) FILTER (WHERE c = 2)
        // FROM t
        // WHERE c = 1 OR c = 2
        //
        // But there are some special cases we need to consider:
        // - The plans to be merged might contain multiple adjacent `Filter` nodes and the parent
        //   `Filter` nodes should incorporate the propagated filters from child ones during merge.
        //   For example, adjacent filters can appear in plans when some of the optimization rules
        //   (like `PushDownPredicates`) are disabled.
        //
        //   Let's consider we want to merge query 1:
        //
        //   SELECT avg(a)
        //   FROM (
        //     SELECT * FROM t WHERE c1 = 1
        //   )
        //   WHERE c2 = 1
        //
        //   and query 2:
        //
        //   SELECT sum(b)
        //   FROM (
        //     SELECT * FROM t WHERE c1 = 2
        //   )
        //   WHERE c2 = 2
        //
        //   Then the optimal merged query is:
        //
        //   SELECT
        //     avg(a) FILTER (WHERE c1 = 1 AND c2 = 1),
        //     sum(b) FILTER (WHERE c1 = 2 AND c2 = 2)
        //   FROM (
        //     SELECT * FROM t WHERE c1 = 1 OR c1 = 2
        //   )
        //   WHERE (c1 = 1 AND c2 = 1) OR (c1 = 2 AND c2 = 2)
        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child, filterPropagationSupported).flatMap {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val mappedNewCondition = mapAttributes(np.condition, outputMap)
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression.
              if (mappedNewCondition.canonicalized == cp.condition.canonicalized) {
                val filters = (mergedChildFilter.toSeq ++ newChildFilter.toSeq).reduceOption(Or)
                  .map(PropagatedFilter)
                val mergedCondition = (filters.toSeq :+ cp.condition).reduce(And)
                val mergedPlan = Filter(mergedCondition, mergedChild)
                val mergeCost = addFilterCost(childMergeCost, mergedCondition,
                  getCost(np.condition), getCost(cp.condition))
                Some(mergedPlan, outputMap, newChildFilter, mergedChildFilter, mergeCost)
              } else if (filterPropagationSupported) {
                val newPlanFilter = (newChildFilter.toSeq :+ mappedNewCondition).reduce(And)
                val cachedPlanFilter = (mergedChildFilter.toSeq :+ cp.condition).reduce(And)
                val mergedCondition = PropagatedFilter(Or(cachedPlanFilter, newPlanFilter))
                val mergedPlan = Filter(mergedCondition, mergedChild)
                val nonPropagatedCachedFilter = extractNonPropagatedFilter(cp.condition)
                val mergedPlanFilter =
                  (mergedChildFilter.toSeq ++ nonPropagatedCachedFilter.toSeq).reduceOption(And)
                val mergeCost = addFilterCost(childMergeCost, mergedCondition,
                  getCost(np.condition), getCost(cp.condition))
                Some(mergedPlan, outputMap, Some(newPlanFilter), mergedPlanFilter, mergeCost)
              } else {
                None
              }
          }
        case (np, cp: Filter) if filterPropagationSupported =>
          tryMergePlans(np, cp.child, true).map {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val nonPropagatedCachedFilter = extractNonPropagatedFilter(cp.condition)
              val mergedPlanFilter =
                (mergedChildFilter.toSeq ++ nonPropagatedCachedFilter.toSeq).reduceOption(And)
              if (newChildFilter.isEmpty) {
                (mergedChild, outputMap, None, mergedPlanFilter, childMergeCost)
              } else {
                val cachedPlanFilter = (mergedChildFilter.toSeq :+ cp.condition).reduce(And)
                val mergedCondition = PropagatedFilter(Or(cachedPlanFilter, newChildFilter.get))
                val mergedPlan = Filter(mergedCondition, mergedChild)
                val mergeCost =
                  addFilterCost(childMergeCost, mergedCondition, 0d, getCost(cp.condition))
                (mergedPlan, outputMap, newChildFilter, mergedPlanFilter, mergeCost)
              }
          }
        case (np: Filter, cp) if filterPropagationSupported =>
          tryMergePlans(np.child, cp, true).map {
            case (mergedChild, outputMap, newChildFilter, mergedChildFilter, childMergeCost) =>
              val mappedNewCondition = mapAttributes(np.condition, outputMap)
              val newPlanFilter = (newChildFilter.toSeq :+ mappedNewCondition).reduce(And)
              if (mergedChildFilter.isEmpty) {
                (mergedChild, outputMap, Some(newPlanFilter), None, childMergeCost)
              } else {
                val mergedCondition = PropagatedFilter(Or(mergedChildFilter.get, newPlanFilter))
                val mergedPlan = Filter(mergedCondition, mergedChild)
                val mergeCost =
                  addFilterCost(childMergeCost, mergedCondition, getCost(np.condition), 0d)
                (mergedPlan, outputMap, Some(newPlanFilter), mergedChildFilter, mergeCost)
              }
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          // Filter propagation is not allowed through joins
          tryMergePlans(np.left, cp.left, false).flatMap {
            case (mergedLeft, leftOutputMap, None, None, _) =>
              tryMergePlans(np.right, cp.right, false).flatMap {
                case (mergedRight, rightOutputMap, None, None, _) =>
                  val outputMap = leftOutputMap ++ rightOutputMap
                  val mappedNewCondition = np.condition.map(mapAttributes(_, outputMap))
                  // Comparing the canonicalized form is required to ignore different forms of the
                  // same expression and `AttributeReference.quailifier`s in `cp.condition`.
                  if (mappedNewCondition.map(_.canonicalized) ==
                    cp.condition.map(_.canonicalized)) {
                    val mergedPlan = cp.withNewChildren(Seq(mergedLeft, mergedRight))
                    Some(mergedPlan, outputMap, None, None, None)
                  } else {
                    None
                  }
                case _ => None
              }
            case _ => None
          }

        // Otherwise merging is not possible.
        case _ => None
      }
    )
  }

  private def createProject(attributes: Seq[Attribute], plan: LogicalPlan): Project = {
    Project(
      Seq(Alias(
        CreateNamedStruct(attributes.flatMap(a => Seq(Literal(a.name), a))),
        "mergedValue")()),
      plan)
  }

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  /**
   * Merges named expression lists of `Project` or `Aggregate` nodes of the new plan into the named
   * expression list of a similar node of the cached plan.
   *
   * - Before we can merge the new expressions we need to take into account the propagated
   * attribute mapping that describes the transformation from the input attributes of the new plan
   * node to the output attributes of the already merged child plan node.
   * - While merging the new expressions we need to build a new attribute mapping to propagate up.
   * - If any filters are propagated from `Filter` nodes below then we could add all the referenced
   * attributes of filter conditions to the merged expression list, but it is better if we alias
   * whole filter conditions and propagate only the new boolean attributes.
   *
   * @param newExpressions    the expression list of the new plan node
   * @param outputMap         the propagated attribute mapping
   * @param cachedExpressions the expression list of the cached plan node
   * @param newChildFilter    the propagated filters from `Filter` nodes of the new plan
   * @param mergedChildFilter the propagated filters from `Filter` nodes of the merged child plan
   * @param childMergeCost    the optional accumulated extra costs of merge
   * @return A tuple of:
   *         - the merged expression list,
   *         - the new attribute mapping to propagate,
   *         - the output attribute of the merged newChildFilter to propagate,
   *         - the output attribute of the merged mergedChildFilter to propagate,
   *         - the extra costs of merging new expressions and filters added to `childMergeCost`
   */
  private def mergeNamedExpressions(
      newExpressions: Seq[NamedExpression],
      outputMap: AttributeMap[Attribute],
      cachedExpressions: Seq[NamedExpression],
      newChildFilter: Option[Expression],
      mergedChildFilter: Option[Expression],
      childMergeCost: Option[Double]):
  (Seq[NamedExpression], AttributeMap[Attribute], Option[Attribute], Option[Attribute],
      Option[Double]) = {
    val mergedExpressions = ArrayBuffer[NamedExpression](cachedExpressions: _*)
    val commonCachedExpressions = mutable.Set.empty[NamedExpression]
    var cachedPlanExtraCost = 0d
    val newOutputMap = AttributeMap(newExpressions.map { ne =>
      val mapped = mapAttributes(ne, outputMap)
      val withoutAlias = mapped match {
        case Alias(child, _) => child
        case e => e
      }
      ne.toAttribute -> mergedExpressions.find {
        case Alias(child, _) => child semanticEquals withoutAlias
        case e => e semanticEquals withoutAlias
      }.map { e =>
        if (childMergeCost.isDefined) {
          commonCachedExpressions += e
        }
        e
      }.getOrElse {
        mergedExpressions += mapped
        if (childMergeCost.isDefined) {
          cachedPlanExtraCost += getCost(mapped)
        }
        mapped
      }.toAttribute
    })

    def mergeFilter(filter: Option[Expression]) = {
      filter.map { f =>
        mergedExpressions.find {
          case Alias(child, _) => child semanticEquals f
          case e => e semanticEquals f
        }.map { e =>
          if (childMergeCost.isDefined) {
            commonCachedExpressions += e
          }
          e
        }.getOrElse {
          val named = f match {
            case ne: NamedExpression => ne
            case o => Alias(o, "propagatedFilter")()
          }
          mergedExpressions += named
          if (childMergeCost.isDefined) {
            cachedPlanExtraCost += getCost(named)
          }
          named
        }.toAttribute
      }
    }

    val mergedPlanFilter = mergeFilter(mergedChildFilter)
    val newPlanFilter = mergeFilter(newChildFilter)

    val mergeCost = childMergeCost.map { c =>
      val newPlanExtraCost = cachedExpressions.collect {
        case e if !commonCachedExpressions.contains(e) => getCost(e)
      }.sum
      c + newPlanExtraCost + cachedPlanExtraCost
    }

    (mergedExpressions.toSeq, newOutputMap, newPlanFilter, mergedPlanFilter, mergeCost)
  }

  /**
   * Adds the extra cost of using `mergedCondition` (instead of the original cost of new and cached
   * plan filter conditions) to the propagated extra cost from merged child plans.
   */
  private def addFilterCost(
      childMergeCost: Option[Double],
      mergedCondition: Expression,
      newPlanFilterCost: Double,
      cachedPlanFilterCost: Double) = {
    childMergeCost.map { c =>
      val mergedConditionCost = getCost(mergedCondition)
      val newPlanExtraCost = mergedConditionCost - newPlanFilterCost
      val cachedPlanExtraCost = mergedConditionCost - cachedPlanFilterCost
      c + newPlanExtraCost + cachedPlanExtraCost
    }
  }

  // Currently only the most basic expressions are supported.
  private def getCost(e: Expression): Double = e match {
    case _: Literal | _: Attribute => 0d
    case PropagatedFilter(child) => getCost(child)
    case Alias(child, _) => getCost(child)
    case _: BinaryComparison | _: BinaryArithmetic | _: And  | _: Or | _: IsNull | _: IsNotNull =>
      1d + e.children.map(getCost).sum
    case _ => Double.PositiveInfinity
  }

  // Only allow aggregates of the same implementation because merging different implementations
  // could cause performance regression.
  private def supportedAggregateMerge(newPlan: Aggregate, cachedPlan: Aggregate) = {
    val aggregateExpressionsSeq = Seq(newPlan, cachedPlan).map { plan =>
      plan.aggregateExpressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
    }
    val Seq(newPlanSupportsHashAggregate, cachedPlanSupportsHashAggregate) =
      aggregateExpressionsSeq.map(aggregateExpressions => ShimUtil.supportsHashAggregate(
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)))
    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
      newPlanSupportsHashAggregate == cachedPlanSupportsHashAggregate && {
        val Seq(newPlanSupportsObjectHashAggregate, cachedPlanSupportsObjectHashAggregate) =
          aggregateExpressionsSeq.map(aggregateExpressions =>
            ShimUtil.supportsObjectHashAggregate(aggregateExpressions))
        newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
          newPlanSupportsObjectHashAggregate == cachedPlanSupportsObjectHashAggregate
      }
  }

  private def extractNonPropagatedFilter(e: Expression) = {
    e match {
      case And(_: PropagatedFilter, e) => Some(e)
      case _: PropagatedFilter => None
      case o => Some(o)
    }
  }

  // We allow filter propagation into aggregates which:
  // - doesn't have grouping expressions and
  // - contains only the most basic aggregate functions.
  private def supportsFilterPropagation(a: Aggregate) = {
    ShimUtil.supportsFilterPropagation(a)
  }

  private def filterAggregateExpressions(
      aggregateExpressions: Seq[NamedExpression],
      filter: Option[Expression]) = {
    if (filter.isDefined) {
      aggregateExpressions.map(_.transform {
        case ae: AggregateExpression =>
          ae.copy(filter = (filter.get +: ae.filter.toSeq).reduceOption(And))
      }.asInstanceOf[NamedExpression])
    } else {
      aggregateExpressions
    }
  }

  private def removePropagatedFilters(plan: LogicalPlan) = {
    plan.transformAllExpressions {
      case pf: PropagatedFilter => pf.child
    }
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(CTERelationRef to the merged plan)` if the plan is merged from
  // multiple subqueries or `ScalarSubquery(original plan)` if it isn't.
  private def removeReferences(
      plan: LogicalPlan,
      cache: ArrayBuffer[Header]) = {
    plan.transformUpWithSubqueries {
      case n =>
        n.transformExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
          case ssr: ScalarSubqueryReference =>
            val header = cache(ssr.subqueryIndex)
            if (header.merged) {
              val subqueryCTE = header.plan.asInstanceOf[CTERelationDef]
              GetStructField(
                ScalarSubquery(
                  ShimUtil.createCTERelationRef(subqueryCTE.id, resolved = true, subqueryCTE.output, subqueryCTE.isStreaming),
                  exprId = ssr.exprId),
                ssr.headerIndex)
            } else {
              ScalarSubquery(header.plan, exprId = ssr.exprId)
            }
        }
    }
  }
}

/**
 * Temporal reference to a cached subquery.
 *
 * @param subqueryIndex A subquery index in the cache.
 * @param headerIndex   An index in the output of merged subquery.
 * @param dataType      The dataType of origin scalar subquery.
 */
case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)

  override def stringArgs: Iterator[Any] = Iterator(subqueryIndex, headerIndex, dataType, exprId.id)
}


/**
 * Temporal wrapper around already propagated predicates.
 */
case class PropagatedFilter(child: Expression) extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType

  override protected def withNewChildInternal(newChild: Expression): PropagatedFilter =
    copy(child = newChild)
}
