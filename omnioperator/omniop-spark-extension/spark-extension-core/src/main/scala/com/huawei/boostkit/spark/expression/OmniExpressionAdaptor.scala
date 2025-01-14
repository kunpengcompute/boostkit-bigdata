/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.expression

import scala.collection.mutable.ArrayBuffer
import com.huawei.boostkit.spark.Constant.{DEFAULT_STRING_TYPE_LENGTH, IS_CHECK_OMNI_EXP, OMNI_BOOLEAN_TYPE, OMNI_DATE_TYPE, OMNI_DECIMAL128_TYPE, OMNI_DECIMAL64_TYPE, OMNI_DOUBLE_TYPE, OMNI_INTEGER_TYPE, OMNI_LONG_TYPE, OMNI_SHOR_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE}
import nova.hetu.omniruntime.`type`.{BooleanDataType, DataTypeSerializer, Date32DataType, Decimal128DataType, Decimal64DataType, DoubleDataType, IntDataType, LongDataType, ShortDataType, TimestampDataType, VarcharDataType}
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.constants.FunctionType.{OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL, OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_SAMP, OMNI_AGGREGATION_TYPE_SUM, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER}
import nova.hetu.omniruntime.constants.JoinType._
import nova.hetu.omniruntime.operator.OmniExprVerify
import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.util.ModifyUtilAdaptor
import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonParser}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.getRawTypeString
import org.apache.spark.sql.execution
import org.apache.spark.sql.hive.HiveUdfAdaptorUtil
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, Metadata, NullType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.util.ShimUtil

import java.util.Locale

object OmniExpressionAdaptor extends Logging {

  def getRealExprId(expr: Expression): ExprId = {
    expr match {
      case alias: Alias => getRealExprId(alias.child)
      case subString: Substring => getRealExprId(subString.str)
      case attr: Attribute => attr.exprId
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  def getExprIdMap(inputAttrs: Seq[Attribute]): Map[ExprId, Int] = {
    var attrMap: Map[ExprId, Int] = Map()
    inputAttrs.zipWithIndex.foreach { case (inputAttr, i) =>
      attrMap += (inputAttr.exprId -> i)
    }
    attrMap
  }

  def checkOmniJsonWhiteList(filterExpr: String, projections: Array[AnyRef]): Unit = {
    if (!IS_CHECK_OMNI_EXP) {
      return
    }
    // inputTypes will not be checked if parseFormat is json( == 1),
    // only if its parseFormat is String (== 0)
    val returnCode: Long = new OmniExprVerify().exprVerifyNative(
      DataTypeSerializer.serialize(new Array[nova.hetu.omniruntime.`type`.DataType](0)),
      0, filterExpr, projections, projections.length, 1)
    if (returnCode == 0) {
      throw new UnsupportedOperationException(s"Unsupported OmniJson Expression \nfilter:${filterExpr}  \nproejcts:${projections.mkString("=")}")
    }
  }

  private val timeFormatSet: Set[String] = Set("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd")
  private val timeZoneSet: Set[String] = Set("GMT+08:00", "Asia/Shanghai")

  private def unsupportedUnixTimeFunction(timeFormat: String, timeZone: String): Unit = {
    if (!ColumnarPluginConfig.getSessionConf.enableOmniUnixTimeFunc) {
      throw new UnsupportedOperationException(s"Not Enabled Omni UnixTime Function")
    }
    if (ColumnarPluginConfig.getSessionConf.timeParserPolicy == "LEGACY") {
      throw new UnsupportedOperationException(s"Unsupported Time Parser Policy: LEGACY")
    }
    if (!timeZoneSet.contains(timeZone)) {
      throw new UnsupportedOperationException(s"Unsupported Time Zone: $timeZone")
    }
    if (!timeFormatSet.contains(timeFormat)) {
      throw new UnsupportedOperationException(s"Unsupported Time Format: $timeFormat")
    }
  }


  def toOmniTimeFormat(format: String): String = {
    format.replace("yyyy", "%Y")
      .replace("MM", "%m")
      .replace("dd", "%d")
      .replace("HH", "%H")
      .replace("mm", "%M")
      .replace("ss", "%S")
  }

  def rewriteToOmniJsonExpressionLiteral(expr: Expression,
                                         exprsIndexMap: Map[ExprId, Int]): String = {
    rewriteToOmniJsonExpressionLiteral(expr, exprsIndexMap, expr.dataType)
  }

  def rewriteToOmniJsonExpressionLiteral(expr: Expression,
                                         exprsIndexMap: Map[ExprId, Int],
                                         returnDatatype: DataType): String = {
    rewriteToOmniJsonExpressionLiteralJsonObject(expr, exprsIndexMap, returnDatatype).toString
  }

  private def rewriteToOmniJsonExpressionLiteralJsonObject(expr: Expression,
                                                           exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    rewriteToOmniJsonExpressionLiteralJsonObject(expr, exprsIndexMap, expr.dataType)
  }

  private def rewriteToOmniJsonExpressionLiteralJsonObject(expr: Expression,
                                                           exprsIndexMap: Map[ExprId, Int],
                                                           returnDatatype: DataType): JsonObject = {
    expr match {
      case subquery: execution.ScalarSubquery =>
        var result: Any = null
        try {
          result = subquery.eval(InternalRow.empty)
        } catch {
          case e: IllegalArgumentException => logDebug(e.getMessage)
        }
        if (result == null) {
          new JsonObject().put("exprType", "LITERAL")
            .addOmniExpJsonType("dataType", subquery.dataType)
            .put("isNull", true)
            .put("value", result)
        } else {
          val literal = Literal(result, subquery.dataType)
          toOmniJsonLiteral(literal)
        }
      case unscaledValue: UnscaledValue =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", unscaledValue.dataType)
          .put("function_name", "UnscaledValue")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(unscaledValue.child, exprsIndexMap)))
      case checkOverflow: CheckOverflow =>
        rewriteToOmniJsonExpressionLiteralJsonObject(checkOverflow.child, exprsIndexMap, returnDatatype)

      case makeDecimal: MakeDecimal =>
        makeDecimal.child.dataType match {
          case decimalChild: DecimalType =>
            new JsonObject().put("exprType", "FUNCTION")
              .addOmniExpJsonType("returnType", makeDecimal.dataType)
              .put("function_name", "MakeDecimal")
              .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(makeDecimal.child, exprsIndexMap)))
          case longChild: LongType =>
            new JsonObject().put("exprType", "FUNCTION")
              .put("function_name", "MakeDecimal")
              .addOmniExpJsonType("returnType", makeDecimal.dataType)
              .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(makeDecimal.child, exprsIndexMap)))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype for MakeDecimal: ${makeDecimal.child.dataType}")
        }

      case sub: Subtract =>
        ShimUtil.unsupportedEvalModeCheck(sub)
        val (left, right) = ShimUtil.binaryOperatorAdjust(sub, returnDatatype)
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", returnDatatype)
          .put("operator", "SUBTRACT")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(right, exprsIndexMap))

      case add: Add =>
        ShimUtil.unsupportedEvalModeCheck(add)
        val (left, right) = ShimUtil.binaryOperatorAdjust(add, returnDatatype)
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", returnDatatype)
          .put("operator", "ADD")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(right, exprsIndexMap))

      case mult: Multiply =>
        ShimUtil.unsupportedEvalModeCheck(mult)
        val (left, right) = ShimUtil.binaryOperatorAdjust(mult, returnDatatype)
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", returnDatatype)
          .put("operator", "MULTIPLY")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(right, exprsIndexMap))

      case divide: Divide =>
        ShimUtil.unsupportedEvalModeCheck(divide)
        val (left, right) = ShimUtil.binaryOperatorAdjust(divide, returnDatatype)
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", returnDatatype)
          .put("operator", "DIVIDE")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(right, exprsIndexMap))

      case mod: Remainder =>
        ShimUtil.unsupportedEvalModeCheck(mod)
        val (left, right) = ShimUtil.binaryOperatorAdjust(mod, returnDatatype)
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", returnDatatype)
          .put("operator", "MODULUS")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(right, exprsIndexMap))

      case greaterThan: GreaterThan =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", greaterThan.dataType)
          .put("operator", "GREATER_THAN")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(greaterThan.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(greaterThan.right, exprsIndexMap))

      case greaterThanOrEq: GreaterThanOrEqual =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", greaterThanOrEq.dataType)
          .put("operator", "GREATER_THAN_OR_EQUAL")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(greaterThanOrEq.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(greaterThanOrEq.right, exprsIndexMap))

      case lessThan: LessThan =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", lessThan.dataType)
          .put("operator", "LESS_THAN")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(lessThan.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(lessThan.right, exprsIndexMap))

      case lessThanOrEq: LessThanOrEqual =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", lessThanOrEq.dataType)
          .put("operator", "LESS_THAN_OR_EQUAL")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(lessThanOrEq.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(lessThanOrEq.right, exprsIndexMap))

      case equal: EqualTo =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", equal.dataType)
          .put("operator", "EQUAL")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(equal.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(equal.right, exprsIndexMap))

      case or: Or =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", or.dataType)
          .put("operator", "OR")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(or.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(or.right, exprsIndexMap))

      case and: And =>
        new JsonObject().put("exprType", "BINARY")
          .addOmniExpJsonType("returnType", and.dataType)
          .put("operator", "AND")
          .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(and.left, exprsIndexMap))
          .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(and.right, exprsIndexMap))

      case alias: Alias => rewriteToOmniJsonExpressionLiteralJsonObject(alias.child, exprsIndexMap)
      case literal: Literal => toOmniJsonLiteral(literal)

      case not: Not =>
        not.child match {
          case isnull: IsNull =>
            new JsonObject().put("exprType", "UNARY")
              .addOmniExpJsonType("returnType", BooleanType)
              .put("operator", "not")
              .put("expr", rewriteToOmniJsonExpressionLiteralJsonObject(isnull, exprsIndexMap))

          case equal: EqualTo =>
            new JsonObject().put("exprType", "BINARY")
              .addOmniExpJsonType("returnType", equal.dataType)
              .put("operator", "NOT_EQUAL")
              .put("left", rewriteToOmniJsonExpressionLiteralJsonObject(equal.left, exprsIndexMap))
              .put("right", rewriteToOmniJsonExpressionLiteralJsonObject(equal.right, exprsIndexMap))

          case _ =>
            new JsonObject().put("exprType", "UNARY")
              .addOmniExpJsonType("returnType", BooleanType)
              .put("operator", "not")
              .put("expr", rewriteToOmniJsonExpressionLiteralJsonObject(not.child, exprsIndexMap))
        }

      case isnotnull: IsNotNull =>
        new JsonObject().put("exprType", "UNARY")
          .addOmniExpJsonType("returnType", BooleanType)
          .put("operator", "not")
          .put("expr", new JsonObject()
            .put("exprType", "IS_NULL")
            .addOmniExpJsonType("returnType", BooleanType)
            .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(isnotnull.child, exprsIndexMap))))

      case isNull: IsNull =>
        new JsonObject().put("exprType", "IS_NULL")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(isNull.child, exprsIndexMap)))
          .addOmniExpJsonType("returnType", BooleanType)

      // Substring
      case subString: Substring =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", subString.dataType)
          .put("function_name", "substr")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(subString.str, exprsIndexMap)).
            put(rewriteToOmniJsonExpressionLiteralJsonObject(subString.pos, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(subString.len, exprsIndexMap)))

      // Cast
      case CastTypeShim(cast) =>
        ShimUtil.unsupportedCastCheck(expr, cast)
        cast.child.dataType match {
          case NullType =>
            // cast(null as xx)的情况，转化为对应的类型的literal下推到omni进行运算
            new JsonObject().put("exprType", "LITERAL")
              .addOmniExpJsonType("dataType", cast.dataType)
              .put("isNull", true)
          case _ =>
            cast.dataType match {
              case StringType =>
                new JsonObject().put("exprType", "FUNCTION")
                  .addOmniExpJsonType("returnType", cast.dataType)
                  .put("width", 50)
                  .put("function_name", "CAST")
                  .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(cast.child, exprsIndexMap)))

              case _ =>
                new JsonObject().put("exprType", "FUNCTION")
                  .addOmniExpJsonType("returnType", cast.dataType)
                  .put("function_name", "CAST")
                  .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(cast.child, exprsIndexMap)))
            }
        }

      // Abs
      case abs: Abs =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", abs.dataType)
          .put("function_name", "abs")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(abs.child, exprsIndexMap)))

      case lower: Lower =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", lower.dataType)
          .put("function_name", "lower")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(lower.child, exprsIndexMap)))

      case upper: Upper =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", upper.dataType)
          .put("function_name", "upper")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(upper.child, exprsIndexMap)))

      case length: Length =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", length.dataType)
          .put("function_name", "length")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(length.child, exprsIndexMap)))

      case replace: StringReplace =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", replace.dataType)
          .put("function_name", "replace")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(replace.srcExpr, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(replace.searchExpr, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(replace.replaceExpr, exprsIndexMap)))

      // In
      case in: In =>
        new JsonObject().put("exprType", "IN")
          .addOmniExpJsonType("returnType", in.dataType)
          .put("arguments", new JsonArray().addFromArray(in.children.map(child => rewriteToOmniJsonExpressionLiteralJsonObject(child, exprsIndexMap)).toArray))

      // coming from In expression with optimizerInSetConversionThreshold
      case inSet: InSet =>
        val jSONObject = new JsonObject().put("exprType", "IN")
          .addOmniExpJsonType("returnType", inSet.dataType)
        val jsonArray = new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(inSet.child, exprsIndexMap))
        inSet.set.foreach(child => jsonArray.put(toOmniJsonLiteral(Literal(child, inSet.child.dataType))))
        jSONObject.put("arguments", jsonArray)
        jSONObject

      case ifExp: If =>
        new JsonObject().put("exprType", "IF")
          .addOmniExpJsonType("returnType", ifExp.dataType)
          .put("condition", rewriteToOmniJsonExpressionLiteralJsonObject(ifExp.predicate, exprsIndexMap))
          .put("if_true", rewriteToOmniJsonExpressionLiteralJsonObject(ifExp.trueValue, exprsIndexMap))
          .put("if_false", rewriteToOmniJsonExpressionLiteralJsonObject(ifExp.falseValue, exprsIndexMap))

      case caseWhen: CaseWhen =>
        procCaseWhenExpression(caseWhen, exprsIndexMap)

      case coalesce: Coalesce =>
        if (coalesce.children.length > 2) {
          throw new UnsupportedOperationException(s"Number of parameters is ${coalesce.children.length}. Exceeds the maximum number of parameters, coalesce only supports up to 2 parameters")
        }
        new JsonObject().put("exprType", "COALESCE")
          .addOmniExpJsonType("returnType", coalesce.dataType)
          .put("value1", rewriteToOmniJsonExpressionLiteralJsonObject(coalesce.children.head, exprsIndexMap))
          .put("value2", rewriteToOmniJsonExpressionLiteralJsonObject(coalesce.children(1), exprsIndexMap))

      case concat: Concat =>
        getConcatJsonStr(concat, exprsIndexMap)
      case greatest: Greatest =>
        getGreatestJsonStr(greatest, exprsIndexMap)

      case round: Round =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", round.dataType)
          .put("function_name", "round")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(round.child, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(round.scale, exprsIndexMap)))

      case attr: Attribute => toOmniJsonLeafExpression(attr, exprsIndexMap(attr.exprId))
      case boundReference: BoundReference =>
        toOmniJsonLeafExpression(boundReference, boundReference.ordinal)

      case hash: Murmur3Hash =>
        genMurMur3HashExpr(hash.children, hash.seed, exprsIndexMap)

      case xxHash: XxHash64 =>
        genXxHash64Expr(xxHash.children, xxHash.seed, exprsIndexMap)

      case inStr: StringInstr =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", inStr.dataType)
          .put("function_name", "instr")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(inStr.str, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(inStr.substr, exprsIndexMap)))

      case rlike: RLike =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", rlike.dataType)
          .put("function_name", "RLike")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(rlike.left, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(rlike.right, exprsIndexMap)))

      // for floating numbers normalize
      case normalizeNaNAndZero: NormalizeNaNAndZero =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", normalizeNaNAndZero.dataType)
          .put("function_name", "NormalizeNaNAndZero")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(normalizeNaNAndZero.child, exprsIndexMap)))

      case knownFloatingPointNormalized: KnownFloatingPointNormalized =>
        rewriteToOmniJsonExpressionLiteralJsonObject(knownFloatingPointNormalized.child, exprsIndexMap)

      // for date time functions
      case unixTimestamp: UnixTimestamp =>
        val timeZone = unixTimestamp.timeZoneId.getOrElse("")
        unsupportedUnixTimeFunction(unixTimestamp.format.toString, timeZone)
        val policy = ColumnarPluginConfig.getSessionConf.timeParserPolicy
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", unixTimestamp.dataType)
          .put("function_name", "unix_timestamp")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(unixTimestamp.timeExp, exprsIndexMap))
            .put(new JsonParser().parse(toOmniTimeFormat(rewriteToOmniJsonExpressionLiteral(unixTimestamp.format, exprsIndexMap))))
            .put(new JsonObject().put("exprType", "LITERAL").put("dataType", 15).put("isNull", timeZone.isEmpty())
              .put("value", timeZone).put("width", timeZone.length))
            .put(new JsonObject().put("exprType", "LITERAL").put("dataType", 15).put("isNull", policy.isEmpty())
              .put("value", policy).put("width", policy.length)))

      case fromUnixTime: FromUnixTime =>
        val timeZone = fromUnixTime.timeZoneId.getOrElse("")
        unsupportedUnixTimeFunction(fromUnixTime.format.toString, timeZone)
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", fromUnixTime.dataType)
          .put("function_name", "from_unixtime")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(fromUnixTime.sec, exprsIndexMap))
            .put(new JsonParser().parse(toOmniTimeFormat(rewriteToOmniJsonExpressionLiteral(fromUnixTime.format, exprsIndexMap))))
            .put(new JsonObject().put("exprType", "LITERAL").put("dataType", 15).put("isNull", timeZone.isEmpty())
                                 .put("value", timeZone).put("width", timeZone.length)))

      // for like
      case startsWith: StartsWith =>
        startsWith.right match {
          case literal: Literal =>
            new JsonObject().put("exprType", "FUNCTION")
              .addOmniExpJsonType("returnType", startsWith.dataType)
              .put("function_name", "StartsWith")
              .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(startsWith.left, exprsIndexMap))
                .put(rewriteToOmniJsonExpressionLiteralJsonObject(startsWith.right, exprsIndexMap)))

          case _ =>
            throw new UnsupportedOperationException(s"Unsupported right expression in like expression: $startsWith")
        }
      case endsWith: EndsWith =>
        endsWith.right match {
          case literal: Literal =>
            new JsonObject().put("exprType", "FUNCTION")
              .addOmniExpJsonType("returnType", endsWith.dataType)
              .put("function_name", "EndsWith")
              .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(endsWith.left, exprsIndexMap))
                .put(rewriteToOmniJsonExpressionLiteralJsonObject(endsWith.right, exprsIndexMap)))

          case _ =>
            throw new UnsupportedOperationException(s"Unsupported right expression in like expression: $endsWith")
        }
      case contains: Contains =>
        contains.children.map(_.dataType).foreach {
          case _: StringType =>
          case dataType =>
            throw new UnsupportedOperationException(s"Invalid input dataType:$dataType for contains")
        }
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", contains.dataType)
          .put("function_name", "Contains")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(contains.left, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(contains.right, exprsIndexMap)))

      case truncDate: TruncDate =>
        new JsonObject().put("exprType", "FUNCTION")
          .addOmniExpJsonType("returnType", truncDate.dataType)
          .put("function_name", "trunc_date")
          .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(truncDate.left, exprsIndexMap))
            .put(rewriteToOmniJsonExpressionLiteralJsonObject(truncDate.right, exprsIndexMap)))

      case md5: Md5 =>
        md5.child match {
          case Cast(inputExpression, outputType, _, _) if outputType == BinaryType =>
            inputExpression match {
              case AttributeReference(_, dataType, _, _) if dataType == StringType =>
                new JsonObject().put("exprType", "FUNCTION")
                  .addOmniExpJsonType("returnType", md5.dataType)
                  .put("function_name", "Md5")
                  .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(inputExpression, exprsIndexMap)))
            }
        }

      case _ =>
        val jsonObj = ModifyUtilAdaptor.rewriteToOmniJsonExpression(expr, exprsIndexMap, returnDatatype, rewriteToOmniJsonExpressionLiteralJsonObject)
        if (jsonObj != null) {
          return jsonObj
        }
        if (HiveUdfAdaptorUtil.isHiveUdf(expr) && ColumnarPluginConfig.getSessionConf.enableColumnarUdf) {
          val hiveUdf = HiveUdfAdaptorUtil.asHiveSimpleUDF(expr)
          val udfName = ShimUtil.parseUdfName(hiveUdf.name)
          return new JsonObject().put("exprType", "FUNCTION")
            .addOmniExpJsonType("returnType", hiveUdf.dataType)
            .put("function_name", udfName)
            .put("arguments", getJsonExprArgumentsByChildren(hiveUdf.children, exprsIndexMap))
        }
        throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  private def getJsonExprArgumentsByChildren(children: Seq[Expression],
                                             exprsIndexMap: Map[ExprId, Int]): JsonArray = {
    val size = children.size
    val jsonArray = new JsonArray()
    if (size == 0) {
      return jsonArray
    }
    for (i <- 0 until size) {
      jsonArray.put(rewriteToOmniJsonExpressionLiteralJsonObject(children(i), exprsIndexMap))
    }
    jsonArray
  }

  private def checkInputDataTypes(children: Seq[Expression]): Unit = {
    val childTypes = children.map(_.dataType)
    for (dataType <- childTypes) {
      if (!dataType.isInstanceOf[StringType]) {
        throw new UnsupportedOperationException(s"Invalid input dataType:$dataType for concat")
      }
    }
  }

  private def getConcatJsonStr(concat: Concat, exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    val children: Seq[Expression] = concat.children
    checkInputDataTypes(children)

    if (children.length == 1) {
      return rewriteToOmniJsonExpressionLiteralJsonObject(children.head, exprsIndexMap)
    }
    val res = new JsonObject().put("exprType", "FUNCTION")
      .addOmniExpJsonType("returnType", concat.dataType)
      .put("function_name", "concat")
      .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(children.head, exprsIndexMap))
        .put(rewriteToOmniJsonExpressionLiteralJsonObject(children(1), exprsIndexMap)))
    for (i <- 2 until children.length) {
      val preResJson = new JsonObject().addAll(res)
      res.put("arguments", new JsonArray().put(preResJson)
        .put(rewriteToOmniJsonExpressionLiteralJsonObject(children(i), exprsIndexMap)))
    }
    res
  }

  private def getGreatestJsonStr(greatest: Greatest, exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    if (greatest.children.length != 2) {
      throw new UnsupportedOperationException(s"Number of parameters is ${greatest.children.length}. " +
        "Greatest only supports up to 2 parameters")
    }

    val children: Seq[Expression] = greatest.children
    children.map(_.dataType).foreach {
      case _: StringType =>
      case _: ShortType =>
      case _: IntegerType =>
      case _: DoubleType =>
      case _: LongType =>
      case _: BooleanType =>
      case _: DecimalType =>
      case _: NullType =>
      case dataType =>
        throw new UnsupportedOperationException(s"Invalid input dataType:$dataType for greatest")
    }
    children.head.dataType match {
      case _: NullType =>
        rewriteToOmniJsonExpressionLiteralJsonObject(children(1), exprsIndexMap)
      case _ => if (children(1).dataType.isInstanceOf[NullType]) {
        rewriteToOmniJsonExpressionLiteralJsonObject(children.head, exprsIndexMap)
      } else {
        children.head match {
          case CastTypeShim(base) if base.child.dataType.isInstanceOf[NullType] =>
            rewriteToOmniJsonExpressionLiteralJsonObject(children(1), exprsIndexMap)
          case _ => children(1) match {
            case CastTypeShim(base) if base.child.dataType.isInstanceOf[NullType] =>
              rewriteToOmniJsonExpressionLiteralJsonObject(children.head, exprsIndexMap)
            case _ =>
              new JsonObject().put("exprType", "FUNCTION")
                .addOmniExpJsonType("returnType", greatest.dataType)
                .put("function_name", "Greatest")
                .put("arguments", new JsonArray().put(rewriteToOmniJsonExpressionLiteralJsonObject(children.head, exprsIndexMap))
                  .put(rewriteToOmniJsonExpressionLiteralJsonObject(children(1), exprsIndexMap)))
          }
        }
      }
    }
  }

  // gen murmur3hash partition expression
  private def genMurMur3HashExpr(expressions: Seq[Expression], seed: Int, exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    var jsonObject: JsonObject = new JsonObject()
    expressions.foreach { expr =>
      val colExprJsonObject = rewriteToOmniJsonExpressionLiteralJsonObject(expr, exprsIndexMap)
      if (jsonObject.entrySet().isEmpty) {
        jsonObject = new JsonObject().put("exprType", "FUNCTION")
          .put("returnType", 1)
          .put("function_name", "mm3hash")
          .put("arguments", new JsonArray()
            .put(colExprJsonObject)
            .put(new JsonObject()
              .put("exprType", "LITERAL")
              .put("dataType", 1)
              .put("isNull", false)
              .put("value", seed)))
      } else {
        jsonObject = new JsonObject().put("exprType", "FUNCTION")
          .put("returnType", 1)
          .put("function_name", "mm3hash")
          .put("arguments", new JsonArray().put(colExprJsonObject).put(jsonObject))
      }
    }
    jsonObject
  }

  // gen XxHash64 partition expression
  private def genXxHash64Expr(expressions: Seq[Expression], seed: Long, exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    var jsonObject: JsonObject = new JsonObject()
    expressions.foreach { expr =>
      val colExprJsonObject = rewriteToOmniJsonExpressionLiteralJsonObject(expr, exprsIndexMap)
      if (jsonObject.entrySet().isEmpty) {
        jsonObject = new JsonObject().put("exprType", "FUNCTION")
          .put("returnType", 2)
          .put("function_name", "xxhash64")
          .put("arguments", new JsonArray()
            .put(colExprJsonObject)
            .put(new JsonObject()
              .put("exprType", "LITERAL")
              .put("dataType", 2)
              .put("isNull", false)
              .put("value", seed)))
      } else {
        jsonObject = new JsonObject().put("exprType", "FUNCTION")
          .put("returnType", 2)
          .put("function_name", "xxhash64")
          .put("arguments", new JsonArray().put(colExprJsonObject).put(jsonObject))
      }
    }
    jsonObject
  }

  def toOmniJsonLeafExpression(attr: LeafExpression, colVal: Int): JsonObject = {
    val omniDataType = sparkTypeToOmniExpType(attr.dataType)
    attr.dataType match {
      case StringType =>
        new JsonObject().put("exprType", "FIELD_REFERENCE")
          .put("dataType", omniDataType.toInt)
          .put("colVal", colVal)
          .put("width", attr match {
            case attribute: Attribute => getStringLength(attribute.metadata)
            case _ => DEFAULT_STRING_TYPE_LENGTH
          })
      case dt: DecimalType =>
        new JsonObject().put("exprType", "FIELD_REFERENCE")
          .put("colVal", colVal)
          .put("dataType", omniDataType.toInt)
          .put("precision", dt.precision)
          .put("scale", dt.scale)
      case _ => new JsonObject().put("exprType", "FIELD_REFERENCE")
        .put("dataType", omniDataType.toInt)
        .put("colVal", colVal)
    }
  }

  def toOmniJsonLiteral(literal: Literal): JsonObject = {
    val omniType = sparkTypeToOmniExpType(literal.dataType)
    val value = literal.value
    if (value == null) {
      return new JsonObject().put("exprType", "LITERAL")
        .addOmniExpJsonType("dataType", literal.dataType)
        .put("isNull", true)
    }
    literal.dataType match {
      case StringType =>
        new JsonObject().put("exprType", "LITERAL")
          .put("dataType", omniType.toInt)
          .put("isNull", false)
          .put("value", value.toString)
          .put("width", value.toString.length)
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          new JsonObject().put("exprType", "LITERAL")
            .put("dataType", omniType.toInt)
            .put("isNull", false)
            .put("value", value.asInstanceOf[Decimal].toUnscaledLong)
            .put("precision", dt.precision)
            .put("scale", dt.scale)
        } else {
          // NOTES: decimal128 literal value need use string format
          new JsonObject().put("exprType", "LITERAL")
            .put("dataType", omniType.toInt)
            .put("isNull", false)
            .put("value", value.asInstanceOf[Decimal].toJavaBigDecimal.unscaledValue().toString())
            .put("precision", dt.precision)
            .put("scale", dt.scale)
        }
      case _ =>
        new JsonObject().put("exprType", "LITERAL")
          .put("dataType", omniType.toInt)
          .put("isNull", false)
          .put("value", value)
    }
  }

  def checkFirstParamType(agg: AggregateExpression): Unit = {
    agg.aggregateFunction.children.map(
      exp => {
        val exprDataType = exp.dataType
        exprDataType match {
          case ShortType =>
          case IntegerType =>
          case LongType =>
          case TimestampType =>
          case DoubleType =>
          case BooleanType =>
          case DateType =>
          case dt: DecimalType =>
          case StringType =>
          case _ =>
            throw new UnsupportedOperationException(s"First_value does not support datatype: $exprDataType")
        }
      }
    )
  }

  def toOmniAggFunType(agg: AggregateExpression, isHashAgg: Boolean = false, isMergeCount: Boolean = false): FunctionType = {
    agg.aggregateFunction match {
      case sum: Sum =>
        ShimUtil.unsupportedEvalModeCheck(sum)
        OMNI_AGGREGATION_TYPE_SUM
      case Max(_) => OMNI_AGGREGATION_TYPE_MAX
      case avg: Average =>
        ShimUtil.unsupportedEvalModeCheck(avg)
        OMNI_AGGREGATION_TYPE_AVG
      case Min(_) => OMNI_AGGREGATION_TYPE_MIN
      case StddevSamp(_, _) => OMNI_AGGREGATION_TYPE_SAMP
      case Count(Literal(1, IntegerType) :: Nil) | Count(ArrayBuffer(Literal(1, IntegerType))) =>
        if (isMergeCount) {
          OMNI_AGGREGATION_TYPE_COUNT_COLUMN
        } else {
          OMNI_AGGREGATION_TYPE_COUNT_ALL
        }
      case Count(_) if agg.aggregateFunction.children.size == 1 => OMNI_AGGREGATION_TYPE_COUNT_COLUMN
      case First(_, true) =>
        checkFirstParamType(agg)
        OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL
      case First(_, false) =>
        checkFirstParamType(agg)
        OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL
      case _ => throw new UnsupportedOperationException(s"Unsupported aggregate function: $agg")
    }
  }

  def toOmniWindowFunType(window: Expression): FunctionType = {
    window match {
      case Rank(_) => OMNI_WINDOW_TYPE_RANK
      case RowNumber() => OMNI_WINDOW_TYPE_ROW_NUMBER
      case _ => throw new UnsupportedOperationException(s"Unsupported window function: $window")
    }
  }

  def toOmniAggInOutJSonExp(attribute: Seq[Expression], exprsIndexMap: Map[ExprId, Int]):
  Array[String] = {
    attribute.map(attr => rewriteToOmniJsonExpressionLiteral(attr, exprsIndexMap)).toArray
  }

  def toOmniAggInOutType(attribute: Seq[AttributeReference]):
  Array[nova.hetu.omniruntime.`type`.DataType] = {
    attribute.map(attr =>
      sparkTypeToOmniType(attr.dataType, attr.metadata)).toArray
  }

  def toOmniAggInOutType(dataType: DataType, metadata: Metadata = Metadata.empty):
  Array[nova.hetu.omniruntime.`type`.DataType] = {
    Array[nova.hetu.omniruntime.`type`.DataType](sparkTypeToOmniType(dataType, metadata))
  }

  def sparkTypeToOmniExpType(datatype: DataType): String = {
    datatype match {
      case ShortType => OMNI_SHOR_TYPE
      case IntegerType => OMNI_INTEGER_TYPE
      case LongType => OMNI_LONG_TYPE
      case DoubleType => OMNI_DOUBLE_TYPE
      case BooleanType => OMNI_BOOLEAN_TYPE
      case StringType => OMNI_VARCHAR_TYPE
      case DateType => OMNI_DATE_TYPE
      case TimestampType => OMNI_TIMESTAMP_TYPE
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          OMNI_DECIMAL64_TYPE
        } else {
          OMNI_DECIMAL128_TYPE
        }
      case NullType => OMNI_BOOLEAN_TYPE
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported datatype: $datatype")
    }
  }

  implicit class JsonObjectExtension(val jsonObject: JsonObject) {
    def addOmniExpJsonType(jsonAttributeKey: String, datatype: DataType): JsonObject = {
      val omniTypeIdStr = sparkTypeToOmniExpType(datatype)
      datatype match {
        case StringType =>
          jsonObject.put(jsonAttributeKey, omniTypeIdStr.toInt)
            .put("width", DEFAULT_STRING_TYPE_LENGTH)
        case dt: DecimalType =>
          jsonObject.put(jsonAttributeKey, omniTypeIdStr.toInt)
            .put("precision", dt.precision)
            .put("scale", dt.scale)
        case _ =>
          jsonObject.put(jsonAttributeKey, omniTypeIdStr.toInt)
      }
      jsonObject
    }

    def addAll(value: JsonObject): JsonObject = {
      value.entrySet().forEach(e => {
        jsonObject.put(e.getKey, e.getValue)
      })
      jsonObject
    }

    def put(jsonAttributeKey: String, value: Any): JsonObject = {
      value match {
        case _: String =>
          jsonObject.addProperty(jsonAttributeKey, value.asInstanceOf[String])
        case _: Number =>
          val d = value.asInstanceOf[Number].doubleValue()
          if (java.lang.Double.isInfinite(d) || java.lang.Double.isNaN(d)) {
            throw new UnsupportedOperationException("Forbidden numeric value: " + d)
          }
          jsonObject.addProperty(jsonAttributeKey, value.asInstanceOf[Number])
        case _: Character =>
          jsonObject.addProperty(jsonAttributeKey, value.asInstanceOf[Character])
        case _: Boolean =>
          jsonObject.addProperty(jsonAttributeKey, value.asInstanceOf[Boolean])
        case _: JsonElement =>
          jsonObject.add(jsonAttributeKey, value.asInstanceOf[JsonElement])
        case _ =>
          // subquery存在value为null的场景，保持原有逻辑，value为null不放入
          if (value != null) {
            throw new UnsupportedOperationException("unsupported value type to be json ")
          }
          jsonObject.remove(jsonAttributeKey)
      }
      jsonObject
    }
  }

  implicit class JsonArrayExtension(val jsonArray: JsonArray) {

    def put(value: JsonElement): JsonArray = {
      jsonArray.add(value)
      jsonArray
    }

    def addFromArray(values: Array[JsonElement]): JsonArray = {
      values.foreach(jsonArray.add)
      jsonArray
    }

  }

  def sparkTypeToOmniType(dataType: DataType): Int = {
    sparkTypeToOmniType(dataType, Metadata.empty).getId.ordinal()
  }

  def sparkTypeToOmniType(dataType: DataType, metadata: Metadata = Metadata.empty):
  nova.hetu.omniruntime.`type`.DataType = {
    dataType match {
      case ShortType =>
        ShortDataType.SHORT
      case IntegerType =>
        IntDataType.INTEGER
      case LongType =>
        LongDataType.LONG
      case TimestampType =>
        TimestampDataType.TIMESTAMP
      case DoubleType =>
        DoubleDataType.DOUBLE
      case BooleanType =>
        BooleanDataType.BOOLEAN
      case StringType =>
        new VarcharDataType(getStringLength(metadata))
      case DateType =>
        Date32DataType.DATE32
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          new Decimal64DataType(dt.precision, dt.scale)
        } else {
          new Decimal128DataType(dt.precision, dt.scale)
        }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported datatype: $dataType")
    }
  }

  def sparkProjectionToOmniJsonProjection(attr: Attribute, colVal: Int): String = {
    val dataType: DataType = attr.dataType
    val metadata = attr.metadata
    val omniDataType: String = sparkTypeToOmniExpType(dataType)
    dataType match {
      case ShortType | IntegerType | LongType | DoubleType | BooleanType | DateType | TimestampType =>
        new JsonObject().put("exprType", "FIELD_REFERENCE")
          .put("dataType", omniDataType.toInt)
          .put("colVal", colVal).toString
      case StringType =>
        new JsonObject().put("exprType", "FIELD_REFERENCE")
          .put("dataType", omniDataType.toInt)
          .put("colVal", colVal)
          .put("width", getStringLength(metadata)).toString
      case dt: DecimalType =>
        var omniDataType = OMNI_DECIMAL128_TYPE
        if (DecimalType.is64BitDecimalType(dt)) {
          omniDataType = OMNI_DECIMAL64_TYPE
        }
        new JsonObject().put("exprType", "FIELD_REFERENCE")
          .put("dataType", omniDataType.toInt)
          .put("colVal", colVal)
          .put("precision", dt.precision)
          .put("scale", dt.scale).toString
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported datatype: $dataType")
    }
  }

  private def getStringLength(metadata: Metadata): Int = {
    var width = DEFAULT_STRING_TYPE_LENGTH
    if (getRawTypeString(metadata).isDefined) {
      val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
      val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r
      val stringOrigDefine = getRawTypeString(metadata).get
      stringOrigDefine match {
        case CHAR_TYPE(length) => width = length.toInt
        case VARCHAR_TYPE(length) => width = length.toInt
        case _ =>
      }
    }
    width
  }

  def procCaseWhenExpression(caseWhen: CaseWhen,
                             exprsIndexMap: Map[ExprId, Int]): JsonObject = {
    var jsonObject = new JsonObject()
    for (i <- caseWhen.branches.indices.reverse) {
      val outerJson = new JsonObject().put("exprType", "IF")
        .addOmniExpJsonType("returnType", caseWhen.dataType)
        .put("condition", rewriteToOmniJsonExpressionLiteralJsonObject(caseWhen.branches(i)._1, exprsIndexMap))
        .put("if_true", rewriteToOmniJsonExpressionLiteralJsonObject(caseWhen.branches(i)._2, exprsIndexMap))

      if (i != caseWhen.branches.length - 1) {
        val innerJson = new JsonObject().addAll(jsonObject)
        outerJson.put("if_false", innerJson)
      } else {
        var elseValue = caseWhen.elseValue
        if (elseValue.isEmpty) {
          elseValue = Some(Literal(null, caseWhen.dataType))
        }
        outerJson.put("if_false", rewriteToOmniJsonExpressionLiteralJsonObject(elseValue.get, exprsIndexMap))
      }
      jsonObject = outerJson
    }
    jsonObject
  }

  def toOmniJoinType(joinType: JoinType): nova.hetu.omniruntime.constants.JoinType = {
    joinType match {
      case FullOuter =>
        OMNI_JOIN_TYPE_FULL
      case Inner =>
        OMNI_JOIN_TYPE_INNER
      case LeftOuter =>
        OMNI_JOIN_TYPE_LEFT
      case RightOuter =>
        OMNI_JOIN_TYPE_RIGHT
      case LeftSemi =>
        OMNI_JOIN_TYPE_LEFT_SEMI
      case LeftAnti =>
        OMNI_JOIN_TYPE_LEFT_ANTI
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[$joinType] is not supported.")
    }
  }

  def isSimpleColumn(expr: String): Boolean = {
    val indexOfExprType = expr.indexOf("exprType")
    val lastIndexOfExprType = expr.lastIndexOf("exprType")
    if (indexOfExprType != -1 && indexOfExprType == lastIndexOfExprType
      && (expr.contains("FIELD_REFERENCE") || expr.contains("LITERAL"))) {
      return true
    }
    false
  }

  def isSimpleColumnForAll(exprArr: Array[String]): Boolean = {
    for (expr <- exprArr) {
      if (!isSimpleColumn(expr)) {
        return false
      }
    }
    true
  }

  def isSimpleProjectForAll(project: NamedExpression): Boolean = {
    project match {
      case attribute: AttributeReference =>
        true
      case alias: Alias =>
        alias.child.isInstanceOf[AttributeReference]
      case _ =>
        false
    }
  }
}
