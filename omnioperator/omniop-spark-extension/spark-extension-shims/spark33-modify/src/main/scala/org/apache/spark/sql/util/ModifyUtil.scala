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

package org.apache.spark.sql.util

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{JsonObjectExtension, JsonArrayExtension}
import com.huawei.boostkit.spark.util.ModifyUtilAdaptor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, ExprId, Expression, PromotePrecision}
import org.apache.spark.sql.catalyst.optimizer.{CombineJoinedAggregates, MergeSubqueryFilters}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.{BroadcastExchangeExecProxy, ColumnarBloomFilterSubquery, ColumnarDataWritingCommandExec, ColumnarToRowExec, OmniColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand, OmniInsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.OmniFileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.orc.{OmniOrcFileFormat, OrcFileFormat}
import org.apache.spark.sql.expression.ColumnarExpressionConverter
import org.apache.spark.sql.types.DataType
import com.google.gson.{JsonArray, JsonObject}

object ModifyUtil extends Logging {

  private def rewriteToOmniJsonExpressionAdapter(expr: Expression,
                                                 exprsIndexMap: Map[ExprId, Int],
                                                 returnDatatype: DataType,
                                                 func: (Expression, Map[ExprId, Int], DataType) => JsonObject): JsonObject = {
    expr match {
      case empty2Null: Empty2Null =>
        new JsonObject().put("exprType", "FUNCTION")
          .put("function_name", "empty2null")
          .addOmniExpJsonType("returnType", empty2Null.dataType)
          .put("arguments", new JsonArray().put(func(empty2Null.child, exprsIndexMap, empty2Null.child.dataType)))

      case promotePrecision: PromotePrecision =>
        func(promotePrecision.child, exprsIndexMap, promotePrecision.child.dataType)

      // might_contain
      case bloomFilterMightContain: BloomFilterMightContain =>
        val expression = ColumnarExpressionConverter.replaceWithColumnarExpression(bloomFilterMightContain.bloomFilterExpression)
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", bloomFilterMightContain.dataType)
          .put("function_name", "might_contain")
          .put("arguments", new JsonArray()
            .put(func(expression, exprsIndexMap, expression.dataType))
            .put(func(bloomFilterMightContain.valueExpression, exprsIndexMap, returnDatatype)))

      case columnarBloomFilterSubquery: ColumnarBloomFilterSubquery =>
        val bfAddress: Long = columnarBloomFilterSubquery.eval().asInstanceOf[Long]
        new JsonObject().put("exprType", "LITERAL")
          .put("isNull", bfAddress == 0L)
          .put("dataType", 2)
          .put("value", bfAddress)

      case _ =>
        null
    }
  }

  private def preReplaceSparkPlanAdapter(plan: SparkPlan, func: (SparkPlan => SparkPlan)): SparkPlan = {
    plan match {
      case plan: DataWritingCommandExec =>
        val child = func(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        var unSupportedColumnarCommand = false
        var unSupportedFileFormat = false
        val omniCmd = plan.cmd match {
          case cmd: InsertIntoHadoopFsRelationCommand =>
            logInfo(s"Columnar Processing for ${cmd.getClass} is currently supported.")
            val fileFormat: FileFormat = cmd.fileFormat match {
              case _: OrcFileFormat => new OmniOrcFileFormat()
              case format =>
                logInfo(s"Unsupported ${format.getClass} file " +
                  s"format for columnar data write command.")
                unSupportedFileFormat = true
                null
            }
            if (unSupportedFileFormat) {
              cmd
            } else {
              OmniInsertIntoHadoopFsRelationCommand(cmd.outputPath, cmd.staticPartitions,
                cmd.ifPartitionNotExists, cmd.partitionColumns, cmd.bucketSpec, fileFormat,
                cmd.options, cmd.query, cmd.mode, cmd.catalogTable,
                cmd.fileIndex, cmd.outputColumnNames
              )
            }
          case cmd: DataWritingCommand =>
            logInfo(s"Columnar Processing for ${cmd.getClass} is currently not supported.")
            unSupportedColumnarCommand = true
            cmd
        }
        if (!unSupportedColumnarCommand && !unSupportedFileFormat) {
          ColumnarDataWritingCommandExec(omniCmd, child)
        } else {
          val children = plan.children.map(func)
          logInfo(s"Columnar Processing for ${plan.getClass} is currently not supported.")
          plan.withNewChildren(children)
        }
      case _ => null
    }
  }

  private def postReplaceSparkPlanAdapter(plan: SparkPlan, func: (SparkPlan => SparkPlan)): SparkPlan = {
    plan match {
      case plan: BroadcastExchangeExecProxy =>
        val children = plan.children.map {
          case c: ColumnarToRowExec =>
            func(c.child)
          case other =>
            func(other)
        }
        plan.withNewChildren(children)
      case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
          c.isInstanceOf[ColumnarToRowExec]) =>
        val children = r.children.map {
          case c: ColumnarToRowExec =>
            val child = func(c.child)
            OmniColumnarToRowExec(child)
          case other =>
            func(other)
        }
        r.withNewChildren(children)
      case _ => null
    }
  }

  private def injectRuleAdapter(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => MergeSubqueryFilters)
    extensions.injectOptimizerRule(_ => CombineJoinedAggregates)
  }

  def registerFunc(): Unit = {
    ModifyUtilAdaptor.configRewriteJsonFunc(rewriteToOmniJsonExpressionAdapter)
    ModifyUtilAdaptor.configPreReplacePlanFunc(preReplaceSparkPlanAdapter)
    ModifyUtilAdaptor.configPostReplacePlanFunc(postReplaceSparkPlanAdapter)
    ModifyUtilAdaptor.configInjectRuleFunc(injectRuleAdapter)
  }
}
