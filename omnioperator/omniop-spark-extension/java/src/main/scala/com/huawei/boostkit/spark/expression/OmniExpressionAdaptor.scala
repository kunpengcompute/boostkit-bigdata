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

import com.huawei.boostkit.spark.Constant.{DEFAULT_STRING_TYPE_LENGTH, IS_CHECK_OMNI_EXP, OMNI_BOOLEAN_TYPE, OMNI_DATE_TYPE, OMNI_DECIMAL128_TYPE, OMNI_DECIMAL64_TYPE, OMNI_DOUBLE_TYPE, OMNI_INTEGER_TYPE, OMNI_LONG_TYPE, OMNI_SHOR_TYPE, OMNI_VARCHAR_TYPE}
import nova.hetu.omniruntime.`type`.{BooleanDataType, DataTypeSerializer, Date32DataType, Decimal128DataType, Decimal64DataType, DoubleDataType, IntDataType, LongDataType, ShortDataType, VarcharDataType}
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.constants.FunctionType.{OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL, OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_SUM, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER}
import nova.hetu.omniruntime.constants.JoinType._
import nova.hetu.omniruntime.operator.OmniExprVerify

import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.getRawTypeString
import org.apache.spark.sql.hive.HiveUdfAdaptorUtil
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, Metadata, ShortType, StringType}

import java.util.Locale
import scala.collection.mutable

object OmniExpressionAdaptor extends Logging {

  def getRealExprId(expr: Expression): ExprId = {
    // TODO support more complex expression
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

  def rewriteToOmniExpressionLiteral(expr: Expression, exprsIndexMap: Map[ExprId, Int]): String = {
    expr match {
      case unscaledValue: UnscaledValue =>
        "UnscaledValue:%s(%s, %d, %d)".format(
          sparkTypeToOmniExpType(unscaledValue.dataType),
          rewriteToOmniExpressionLiteral(unscaledValue.child, exprsIndexMap),
          unscaledValue.child.dataType.asInstanceOf[DecimalType].precision,
          unscaledValue.child.dataType.asInstanceOf[DecimalType].scale)

      // omni not support return null, now rewrite to if(IsOverflowDecimal())? NULL:MakeDecimal()
      case checkOverflow: CheckOverflow =>
        ("IF:%s(IsOverflowDecimal:%s(%s,%d,%d,%d,%d), %s, MakeDecimal:%s(%s,%d,%d,%d,%d))")
          .format(sparkTypeToOmniExpType(checkOverflow.dataType),
            // IsOverflowDecimal returnType
            sparkTypeToOmniExpType(BooleanType),
            // IsOverflowDecimal arguments
            rewriteToOmniExpressionLiteral(checkOverflow.child, exprsIndexMap),
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            // if_true
            rewriteToOmniExpressionLiteral(Literal(null, checkOverflow.dataType), exprsIndexMap),
            // if_false
            sparkTypeToOmniExpJsonType(checkOverflow.dataType),
            rewriteToOmniExpressionLiteral(checkOverflow.child, exprsIndexMap),
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            checkOverflow.dataType.precision, checkOverflow.dataType.scale)

      case makeDecimal: MakeDecimal =>
        makeDecimal.child.dataType match {
          case decimalChild: DecimalType =>
            ("MakeDecimal:%s(%s,%s,%s,%s,%s)")
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniExpressionLiteral(makeDecimal.child, exprsIndexMap),
                decimalChild.precision, decimalChild.scale,
                makeDecimal.precision, makeDecimal.scale)
          case longChild: LongType =>
            ("MakeDecimal:%s(%s,%s,%s)")
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniExpressionLiteral(makeDecimal.child, exprsIndexMap),
                makeDecimal.precision, makeDecimal.scale)
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype for MakeDecimal: ${makeDecimal.child.dataType}")
        }

      case promotePrecision: PromotePrecision =>
        rewriteToOmniExpressionLiteral(promotePrecision.child, exprsIndexMap)

      case sub: Subtract =>
        "$operator$SUBTRACT:%s(%s,%s)".format(
          sparkTypeToOmniExpType(sub.dataType),
          rewriteToOmniExpressionLiteral(sub.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(sub.right, exprsIndexMap))

      case add: Add =>
        "$operator$ADD:%s(%s,%s)".format(
          sparkTypeToOmniExpType(add.dataType),
          rewriteToOmniExpressionLiteral(add.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(add.right, exprsIndexMap))

      case mult: Multiply =>
        "$operator$MULTIPLY:%s(%s,%s)".format(
          sparkTypeToOmniExpType(mult.dataType),
          rewriteToOmniExpressionLiteral(mult.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(mult.right, exprsIndexMap))

      case divide: Divide =>
        "$operator$DIVIDE:%s(%s,%s)".format(
          sparkTypeToOmniExpType(divide.dataType),
          rewriteToOmniExpressionLiteral(divide.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(divide.right, exprsIndexMap))

      case mod: Remainder =>
        "$operator$MODULUS:%s(%s,%s)".format(
          sparkTypeToOmniExpType(mod.dataType),
          rewriteToOmniExpressionLiteral(mod.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(mod.right, exprsIndexMap))

      case greaterThan: GreaterThan =>
        "$operator$GREATER_THAN:%s(%s,%s)".format(
          sparkTypeToOmniExpType(greaterThan.dataType),
          rewriteToOmniExpressionLiteral(greaterThan.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(greaterThan.right, exprsIndexMap))

      case greaterThanOrEq: GreaterThanOrEqual =>
        "$operator$GREATER_THAN_OR_EQUAL:%s(%s,%s)".format(
          sparkTypeToOmniExpType(greaterThanOrEq.dataType),
          rewriteToOmniExpressionLiteral(greaterThanOrEq.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(greaterThanOrEq.right, exprsIndexMap))

      case lessThan: LessThan =>
        "$operator$LESS_THAN:%s(%s,%s)".format(
          sparkTypeToOmniExpType(lessThan.dataType),
          rewriteToOmniExpressionLiteral(lessThan.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(lessThan.right, exprsIndexMap))

      case lessThanOrEq: LessThanOrEqual =>
        "$operator$LESS_THAN_OR_EQUAL:%s(%s,%s)".format(
          sparkTypeToOmniExpType(lessThanOrEq.dataType),
          rewriteToOmniExpressionLiteral(lessThanOrEq.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(lessThanOrEq.right, exprsIndexMap))

      case equal: EqualTo =>
        "$operator$EQUAL:%s(%s,%s)".format(
          sparkTypeToOmniExpType(equal.dataType),
          rewriteToOmniExpressionLiteral(equal.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(equal.right, exprsIndexMap))

      case or: Or =>
        "OR:%s(%s,%s)".format(
          sparkTypeToOmniExpType(or.dataType),
          rewriteToOmniExpressionLiteral(or.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(or.right, exprsIndexMap))

      case and: And =>
        "AND:%s(%s,%s)".format(
          sparkTypeToOmniExpType(and.dataType),
          rewriteToOmniExpressionLiteral(and.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(and.right, exprsIndexMap))

      case alias: Alias => rewriteToOmniExpressionLiteral(alias.child, exprsIndexMap)
      case literal: Literal => toOmniLiteral(literal)
      case not: Not =>
        "not:%s(%s)".format(
          sparkTypeToOmniExpType(BooleanType),
          rewriteToOmniExpressionLiteral(not.child, exprsIndexMap))
      case isnotnull: IsNotNull =>
        "IS_NOT_NULL:%s(%s)".format(
          sparkTypeToOmniExpType(BooleanType),
          rewriteToOmniExpressionLiteral(isnotnull.child, exprsIndexMap))
      // Substring
      case subString: Substring =>
        "substr:%s(%s,%s,%s)".format(
          sparkTypeToOmniExpType(subString.dataType),
          rewriteToOmniExpressionLiteral(subString.str, exprsIndexMap),
          rewriteToOmniExpressionLiteral(subString.pos, exprsIndexMap),
          rewriteToOmniExpressionLiteral(subString.len, exprsIndexMap))
      // Cast
      case cast: Cast =>
        unsupportedCastCheck(expr, cast)
        "CAST:%s(%s)".format(
          sparkTypeToOmniExpType(cast.dataType),
          rewriteToOmniExpressionLiteral(cast.child, exprsIndexMap))
      // Abs
      case abs: Abs =>
        "abs:%s(%s)".format(
          sparkTypeToOmniExpType(abs.dataType),
          rewriteToOmniExpressionLiteral(abs.child, exprsIndexMap))
      // In
      case in: In =>
        "IN:%s(%s)".format(
          sparkTypeToOmniExpType(in.dataType),
          in.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))
      // coming from In expression with optimizerInSetConversionThreshold
      case inSet: InSet =>
        "IN:%s(%s,%s)".format(
          sparkTypeToOmniExpType(inSet.dataType),
          rewriteToOmniExpressionLiteral(inSet.child, exprsIndexMap),
          inSet.set.map(child => toOmniLiteral(
            Literal(child, inSet.child.dataType))).mkString(","))
      // only support with one case condition, for omni rewrite to if(A, B, C)
      case caseWhen: CaseWhen =>
        "IF:%s(%s, %s, %s)".format(
          sparkTypeToOmniExpType(caseWhen.dataType),
          rewriteToOmniExpressionLiteral(caseWhen.branches(0)._1, exprsIndexMap),
          rewriteToOmniExpressionLiteral(caseWhen.branches(0)._2, exprsIndexMap),
          rewriteToOmniExpressionLiteral(caseWhen.elseValue.get, exprsIndexMap))
      // Sum
      case sum: Sum =>
        "SUM:%s(%s)".format(
          sparkTypeToOmniExpType(sum.dataType),
          sum.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))
      // Max
      case max: Max =>
        "MAX:%s(%s)".format(
          sparkTypeToOmniExpType(max.dataType),
          max.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))
      // Average
      case avg: Average =>
        "AVG:%s(%s)".format(
          sparkTypeToOmniExpType(avg.dataType),
          avg.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))
      // Min
      case min: Min =>
        "MIN:%s(%s)".format(
          sparkTypeToOmniExpType(min.dataType),
          min.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))

      case coalesce: Coalesce =>
        "COALESCE:%s(%s)".format(
          sparkTypeToOmniExpType(coalesce.dataType),
          coalesce.children.map(child => rewriteToOmniExpressionLiteral(child, exprsIndexMap))
            .mkString(","))

      case concat: Concat =>
        getConcatStr(concat, exprsIndexMap)

      case attr: Attribute => s"#${exprsIndexMap(attr.exprId).toString}"
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  private def getConcatStr(concat: Concat, exprsIndexMap: Map[ExprId, Int]): String = {
    val child: Seq[Expression] = concat.children
    checkInputDataTypes(child)
    val template = "concat:%s(%s,%s)"
    val omniType = sparkTypeToOmniExpType(concat.dataType)
    if (child.length == 1) {
      return rewriteToOmniExpressionLiteral(child.head, exprsIndexMap)
    }
    // (a, b, c) => concat(concat(a,b),c)
    var res = template.format(omniType,
      rewriteToOmniExpressionLiteral(child.head, exprsIndexMap),
      rewriteToOmniExpressionLiteral(child(1), exprsIndexMap))
    for (i <- 2 until child.length) {
      res = template.format(omniType, res,
        rewriteToOmniExpressionLiteral(child(i), exprsIndexMap))
    }
    res
  }

  private def unsupportedCastCheck(expr: Expression, cast: Cast): Unit = {
    def isDecimalOrStringType(dataType: DataType): Boolean = (dataType.isInstanceOf[DecimalType]) || (dataType.isInstanceOf[StringType])
    // not support Cast(string as !(decimal/string)) and Cast(!(decimal/string) as string)
    if ((cast.dataType.isInstanceOf[StringType] && !isDecimalOrStringType(cast.child.dataType)) ||
      (!isDecimalOrStringType(cast.dataType) && cast.child.dataType.isInstanceOf[StringType])) {
      throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  def toOmniLiteral(literal: Literal): String = {
    val omniType = sparkTypeToOmniExpType(literal.dataType)
    literal.dataType match {
      case null => s"null:${omniType}"
      case StringType => s"\'${literal.toString}\':${omniType}"
      case _ => literal.toString + s":${omniType}"
    }
  }

  def rewriteToOmniJsonExpressionLiteral(expr: Expression,
                                              exprsIndexMap: Map[ExprId, Int]): String = {
    rewriteToOmniJsonExpressionLiteral(expr, exprsIndexMap, expr.dataType)
  }

  def rewriteToOmniJsonExpressionLiteral(expr: Expression,
                                         exprsIndexMap: Map[ExprId, Int],
                                         returnDatatype: DataType): String = {
    expr match {
      case unscaledValue: UnscaledValue =>
        ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
          "\"function_name\":\"UnscaledValue\", \"arguments\":[%s]}")
          .format(sparkTypeToOmniExpJsonType(unscaledValue.dataType),
            rewriteToOmniJsonExpressionLiteral(unscaledValue.child, exprsIndexMap))

      case checkOverflow: CheckOverflow =>
        rewriteToOmniJsonExpressionLiteral(checkOverflow.child, exprsIndexMap, returnDatatype)

      case makeDecimal: MakeDecimal =>
        makeDecimal.child.dataType match {
          case decimalChild: DecimalType =>
            ("{\"exprType\": \"FUNCTION\", \"returnType\":%s," +
              "\"function_name\": \"MakeDecimal\", \"arguments\": [%s]}")
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniJsonExpressionLiteral(makeDecimal.child, exprsIndexMap))

          case longChild: LongType =>
            ("{\"exprType\": \"FUNCTION\", \"returnType\":%s," +
              "\"function_name\": \"MakeDecimal\", \"arguments\": [%s]}")
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniJsonExpressionLiteral(makeDecimal.child, exprsIndexMap))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype for MakeDecimal: ${makeDecimal.child.dataType}")
        }

      case promotePrecision: PromotePrecision =>
        rewriteToOmniJsonExpressionLiteral(promotePrecision.child, exprsIndexMap)

      case sub: Subtract =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"SUBTRACT\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(returnDatatype),
            rewriteToOmniJsonExpressionLiteral(sub.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(sub.right, exprsIndexMap))

      case add: Add =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"ADD\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(returnDatatype),
            rewriteToOmniJsonExpressionLiteral(add.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(add.right, exprsIndexMap))

      case mult: Multiply =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"MULTIPLY\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(returnDatatype),
            rewriteToOmniJsonExpressionLiteral(mult.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(mult.right, exprsIndexMap))

      case divide: Divide =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"DIVIDE\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(returnDatatype),
            rewriteToOmniJsonExpressionLiteral(divide.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(divide.right, exprsIndexMap))

      case mod: Remainder =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"MODULUS\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(returnDatatype),
            rewriteToOmniJsonExpressionLiteral(mod.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(mod.right, exprsIndexMap))

      case greaterThan: GreaterThan =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"GREATER_THAN\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(greaterThan.dataType),
            rewriteToOmniJsonExpressionLiteral(greaterThan.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(greaterThan.right, exprsIndexMap))

      case greaterThanOrEq: GreaterThanOrEqual =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"GREATER_THAN_OR_EQUAL\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(greaterThanOrEq.dataType),
            rewriteToOmniJsonExpressionLiteral(greaterThanOrEq.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(greaterThanOrEq.right, exprsIndexMap))

      case lessThan: LessThan =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"LESS_THAN\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(lessThan.dataType),
            rewriteToOmniJsonExpressionLiteral(lessThan.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(lessThan.right, exprsIndexMap))

      case lessThanOrEq: LessThanOrEqual =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"LESS_THAN_OR_EQUAL\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(lessThanOrEq.dataType),
            rewriteToOmniJsonExpressionLiteral(lessThanOrEq.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(lessThanOrEq.right, exprsIndexMap))

      case equal: EqualTo =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"EQUAL\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(equal.dataType),
            rewriteToOmniJsonExpressionLiteral(equal.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(equal.right, exprsIndexMap))

      case or: Or =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"OR\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(or.dataType),
            rewriteToOmniJsonExpressionLiteral(or.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(or.right, exprsIndexMap))

      case and: And =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
          "\"operator\":\"AND\",\"left\":%s,\"right\":%s}").format(
            sparkTypeToOmniExpJsonType(and.dataType),
            rewriteToOmniJsonExpressionLiteral(and.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(and.right, exprsIndexMap))

      case alias: Alias => rewriteToOmniJsonExpressionLiteral(alias.child, exprsIndexMap)
      case literal: Literal => toOmniJsonLiteral(literal)
      case not: Not =>
        not.child match {
          case isnull: IsNull =>
            "{\"exprType\":\"UNARY\",\"returnType\":%s,\"operator\":\"not\",\"expr\":%s}".format(
              sparkTypeToOmniExpJsonType(BooleanType),
              rewriteToOmniJsonExpressionLiteral(isnull, exprsIndexMap))
          case equal: EqualTo =>
            ("{\"exprType\":\"BINARY\",\"returnType\":%s," +
              "\"operator\":\"NOT_EQUAL\",\"left\":%s,\"right\":%s}").format(
              sparkTypeToOmniExpJsonType(equal.dataType),
              rewriteToOmniJsonExpressionLiteral(equal.left, exprsIndexMap),
              rewriteToOmniJsonExpressionLiteral(equal.right, exprsIndexMap))
          case _ => throw new UnsupportedOperationException(s"Unsupported expression: $expr")
        }
      case isnotnull: IsNotNull =>
        ("{\"exprType\":\"UNARY\",\"returnType\":%s, \"operator\":\"not\","
          + "\"expr\":{\"exprType\":\"IS_NULL\",\"returnType\":%s,"
          + "\"arguments\":[%s]}}").format(sparkTypeToOmniExpJsonType(BooleanType),
          sparkTypeToOmniExpJsonType(BooleanType),
          rewriteToOmniJsonExpressionLiteral(isnotnull.child, exprsIndexMap))

      case isNull: IsNull =>
         "{\"exprType\":\"IS_NULL\",\"returnType\":%s,\"arguments\":[%s]}".format(
          sparkTypeToOmniExpJsonType(BooleanType),
          rewriteToOmniJsonExpressionLiteral(isNull.child, exprsIndexMap))

      // Substring
      case subString: Substring =>
        ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
          "\"function_name\":\"substr\", \"arguments\":[%s,%s,%s]}")
          .format(sparkTypeToOmniExpJsonType(subString.dataType),
          rewriteToOmniJsonExpressionLiteral(subString.str, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(subString.pos, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(subString.len, exprsIndexMap))

      // Cast
      case cast: Cast =>
        unsupportedCastCheck(expr, cast)
        val returnType = sparkTypeToOmniExpJsonType(cast.dataType)
        cast.dataType match {
          case StringType =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"width\":50,\"function_name\":\"CAST\", \"arguments\":[%s]}")
              .format(returnType, rewriteToOmniJsonExpressionLiteral(cast.child, exprsIndexMap))
          case _ =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"function_name\":\"CAST\",\"arguments\":[%s]}")
              .format(returnType, rewriteToOmniJsonExpressionLiteral(cast.child, exprsIndexMap))
        }
      // Abs
      case abs: Abs =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"abs\", \"arguments\":[%s]}"
          .format(sparkTypeToOmniExpJsonType(abs.dataType),
            rewriteToOmniJsonExpressionLiteral(abs.child, exprsIndexMap))

      case lower: Lower =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"lower\", \"arguments\":[%s]}"
          .format(sparkTypeToOmniExpJsonType(lower.dataType),
            rewriteToOmniJsonExpressionLiteral(lower.child, exprsIndexMap))

      case length: Length =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"length\", \"arguments\":[%s]}"
          .format(sparkTypeToOmniExpJsonType(length.dataType),
            rewriteToOmniJsonExpressionLiteral(length.child, exprsIndexMap))

      case replace: StringReplace =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"replace\", \"arguments\":[%s,%s,%s]}"
          .format(sparkTypeToOmniExpJsonType(replace.dataType),
            rewriteToOmniJsonExpressionLiteral(replace.srcExpr, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(replace.searchExpr, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(replace.replaceExpr, exprsIndexMap))

      // In
      case in: In =>
        "{\"exprType\":\"IN\",\"returnType\":%s, \"arguments\":%s}".format(
          sparkTypeToOmniExpJsonType(in.dataType),
          in.children.map(child => rewriteToOmniJsonExpressionLiteral(child, exprsIndexMap))
            .mkString("[", ",", "]"))

      // coming from In expression with optimizerInSetConversionThreshold
      case inSet: InSet =>
        "{\"exprType\":\"IN\",\"returnType\":%s, \"arguments\":[%s, %s]}"
          .format(sparkTypeToOmniExpJsonType(inSet.dataType),
            rewriteToOmniJsonExpressionLiteral(inSet.child, exprsIndexMap),
            inSet.set.map(child =>
              toOmniJsonLiteral(Literal(child, inSet.child.dataType))).mkString(","))

      case ifExp: If =>
        "{\"exprType\":\"IF\",\"returnType\":%s,\"condition\":%s,\"if_true\":%s,\"if_false\":%s}"
          .format(sparkTypeToOmniExpJsonType(ifExp.dataType),
            rewriteToOmniJsonExpressionLiteral(ifExp.predicate, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(ifExp.trueValue, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(ifExp.falseValue, exprsIndexMap))

      case caseWhen: CaseWhen =>
        procCaseWhenExpression(caseWhen, exprsIndexMap)

      case coalesce: Coalesce =>
        if (coalesce.children.length > 2) {
           throw new UnsupportedOperationException(s"Number of parameters is ${coalesce.children.length}. Exceeds the maximum number of parameters, coalesce only supports up to 2 parameters")
        }
        "{\"exprType\":\"COALESCE\",\"returnType\":%s, \"value1\":%s,\"value2\":%s}".format(
          sparkTypeToOmniExpJsonType(coalesce.dataType),
          rewriteToOmniJsonExpressionLiteral(coalesce.children(0), exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(coalesce.children(1), exprsIndexMap))

      case concat: Concat =>
        getConcatJsonStr(concat, exprsIndexMap)
      case attr: Attribute => toOmniJsonAttribute(attr, exprsIndexMap(attr.exprId))
      case _ =>
        if (HiveUdfAdaptorUtil.isHiveUdf(expr) && ColumnarPluginConfig.getSessionConf.enableColumnarUdf) {
          val hiveUdf = HiveUdfAdaptorUtil.asHiveSimpleUDF(expr)
          val nameSplit = hiveUdf.name.split("\\.")
          val udfName = if (nameSplit.size == 1) nameSplit(0).toLowerCase(Locale.ROOT) else nameSplit(1).toLowerCase(Locale.ROOT)
          return ("{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"%s\"," +
            "\"arguments\":[%s]}").format(sparkTypeToOmniExpJsonType(hiveUdf.dataType), udfName,
            getJsonExprArgumentsByChildren(hiveUdf.children, exprsIndexMap))
        }
        throw new UnsupportedOperationException(s"Unsupported expression: $expr")
    }
  }

  private def getJsonExprArgumentsByChildren(children: Seq[Expression],
                                             exprsIndexMap: Map[ExprId, Int]): String = {
    val size = children.size
    val stringBuild = new mutable.StringBuilder
    if (size == 0) {
      return stringBuild.toString()
    }
    for (i <- 0 until size - 1) {
      stringBuild.append(rewriteToOmniJsonExpressionLiteral(children(i), exprsIndexMap))
      stringBuild.append(",")
    }
    stringBuild.append(rewriteToOmniJsonExpressionLiteral(children(size - 1), exprsIndexMap))
    stringBuild.toString()
  }

  private def checkInputDataTypes(children: Seq[Expression]): Unit = {
    val childTypes = children.map(_.dataType)
    for (dataType <- childTypes) {
      if (!dataType.isInstanceOf[StringType]) {
        throw new UnsupportedOperationException(s"Invalid input dataType:$dataType for concat")
      }
    }
  }

  private def getConcatJsonStr(concat: Concat, exprsIndexMap: Map[ExprId, Int]): String = {
    val children: Seq[Expression] = concat.children
    checkInputDataTypes(children)
    val template = "{\"exprType\": \"FUNCTION\",\"returnType\":%s," +
      "\"function_name\": \"concat\", \"arguments\": [%s, %s]}"
    val returnType = sparkTypeToOmniExpJsonType(concat.dataType)
    if (children.length == 1) {
      return rewriteToOmniJsonExpressionLiteral(children.head, exprsIndexMap)
    }
    var res = template.format(returnType,
      rewriteToOmniJsonExpressionLiteral(children.head, exprsIndexMap),
      rewriteToOmniJsonExpressionLiteral(children(1), exprsIndexMap))
    for (i <- 2 until children.length) {
      res = template.format(returnType, res,
        rewriteToOmniJsonExpressionLiteral(children(i), exprsIndexMap))
    }
    res
  }

  def toOmniJsonAttribute(attr: Attribute, colVal: Int): String = {

  val omniDataType = sparkTypeToOmniExpType(attr.dataType)
    attr.dataType match {
      case StringType =>
        ("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s," +
          "\"colVal\":%d,\"width\":%d}").format(omniDataType, colVal,
          getStringLength(attr.metadata))
      case dt: DecimalType =>
        ("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s," +
          "\"colVal\":%d,\"precision\":%s, \"scale\":%s}").format(omniDataType,
          colVal, dt.precision, dt.scale)
      case _ => ("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s," +
          "\"colVal\":%d}").format(omniDataType, colVal)
    }
  }

  def toOmniJsonLiteral(literal: Literal): String = {
    val omniType = sparkTypeToOmniExpType(literal.dataType)
    val value = literal.value
    if (value == null) {
      return "{\"exprType\":\"LITERAL\",\"dataType\":%s,\"isNull\":%b}".format(sparkTypeToOmniExpJsonType(literal.dataType), true)
    }
    literal.dataType match {
      case StringType =>
        ("{\"exprType\":\"LITERAL\",\"dataType\":%s," +
          "\"isNull\":%b, \"value\":\"%s\",\"width\":%d}")
          .format(omniType, false, value.toString, value.toString.length)
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          ("{\"exprType\":\"LITERAL\",\"dataType\":%s," +
            "\"isNull\":%b,\"value\":%s,\"precision\":%s, \"scale\":%s}").format(omniType,
            false, value.asInstanceOf[Decimal].toUnscaledLong, dt.precision, dt.scale)
        } else {
          // NOTES: decimal128 literal value need use string format
          ("{\"exprType\":\"LITERAL\",\"dataType\":%s," +
            "\"isNull\":%b, \"value\":\"%s\", \"precision\":%s, \"scale\":%s}").format(omniType,
            false, value.asInstanceOf[Decimal].toJavaBigDecimal.unscaledValue().toString(),
            dt.precision, dt.scale)
        }
      case _ =>
        "{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b, \"value\":%s}"
          .format(omniType, false, value)
    }
  }

  def toOmniAggFunType(agg: AggregateExpression, isHashAgg: Boolean = false, isFinal: Boolean = false): FunctionType = {
    agg.aggregateFunction match {
      case Sum(_) => OMNI_AGGREGATION_TYPE_SUM
      case Max(_) => OMNI_AGGREGATION_TYPE_MAX
      case Average(_) => OMNI_AGGREGATION_TYPE_AVG
      case Min(_) => OMNI_AGGREGATION_TYPE_MIN
      case Count(Literal(1, IntegerType) :: Nil) | Count(ArrayBuffer(Literal(1, IntegerType))) =>
        if (isFinal) {
          OMNI_AGGREGATION_TYPE_COUNT_COLUMN
        } else {
          OMNI_AGGREGATION_TYPE_COUNT_ALL
        }
      case Count(_) => OMNI_AGGREGATION_TYPE_COUNT_COLUMN
      case First(_, true) => OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL
      case First(_, false) => OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL
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
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          OMNI_DECIMAL64_TYPE
        } else {
          OMNI_DECIMAL128_TYPE
        }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported datatype: $datatype")
    }
  }

  def sparkTypeToOmniExpJsonType(datatype: DataType): String = {
    val omniTypeIdStr = sparkTypeToOmniExpType(datatype)
    datatype match {
      case StringType =>
        "%s,\"width\":%s".format(omniTypeIdStr, DEFAULT_STRING_TYPE_LENGTH)
      case dt: DecimalType =>
        "%s,\"precision\":%s,\"scale\":%s".format(omniTypeIdStr, dt.precision, dt.scale)
      case _ =>
        omniTypeIdStr
    }
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
      case ShortType | IntegerType | LongType | DoubleType | BooleanType | DateType =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s,\"colVal\":%d}"
          .format(omniDataType, colVal)
      case StringType =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s,\"colVal\":%d,\"width\":%d}"
          .format(omniDataType, colVal, getStringLength(metadata))
      case dt: DecimalType =>
        var omniDataType = OMNI_DECIMAL128_TYPE
        if (DecimalType.is64BitDecimalType(dt)) {
          omniDataType = OMNI_DECIMAL64_TYPE
        }
        ("{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s,\"colVal\":%d," +
          "\"precision\":%s,\"scale\":%s}")
          .format(omniDataType, colVal, dt.precision, dt.scale)
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
                             exprsIndexMap: Map[ExprId, Int]): String = {
    val exprStr = "{\"exprType\":\"IF\",\"returnType\":%s,\"condition\":%s,\"if_true\":%s,\"if_false\":%s}"
    var exprStrRes = exprStr
    for (i <- caseWhen.branches.indices) {
      var ifFalseStr = ""
      if (i != caseWhen.branches.length - 1) {
        ifFalseStr = exprStr
      } else {
        var elseValue = caseWhen.elseValue
        if (elseValue.isEmpty) {
           elseValue = Some(Literal(null, caseWhen.dataType))
        }
        ifFalseStr = rewriteToOmniJsonExpressionLiteral(elseValue.get, exprsIndexMap)
      }
      exprStrRes = exprStrRes.format(sparkTypeToOmniExpJsonType(caseWhen.dataType),
        rewriteToOmniJsonExpressionLiteral(caseWhen.branches(i)._1, exprsIndexMap),
        rewriteToOmniJsonExpressionLiteral(caseWhen.branches(i)._2, exprsIndexMap),
        ifFalseStr)
    }
    exprStrRes
  }

  def procLikeExpression(likeExpr: Expression,
                         exprsIndexMap: Map[ExprId, Int]): String = {
    likeExpr match {
      case like: Like =>
        val dataType = like.right.dataType
        like.right match {
          case literal: Literal =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"function_name\":\"LIKE\", \"arguments\":[%s, %s]}")
              .format(sparkTypeToOmniExpJsonType(like.dataType),
                rewriteToOmniJsonExpressionLiteral(like.left, exprsIndexMap),
                generateLikeArg(literal,""))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype in like expression: $dataType")
        }
      case startsWith: StartsWith =>
        val dataType = startsWith.right.dataType
        startsWith.right match {
          case literal: Literal =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"function_name\":\"LIKE\", \"arguments\":[%s, %s]}")
              .format(sparkTypeToOmniExpJsonType(startsWith.dataType),
                rewriteToOmniJsonExpressionLiteral(startsWith.left, exprsIndexMap),
                generateLikeArg(literal, "startsWith"))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype in like expression: $dataType")
        }
      case endsWith: EndsWith =>
        val dataType = endsWith.right.dataType
        endsWith.right match {
          case literal: Literal =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"function_name\":\"LIKE\", \"arguments\":[%s, %s]}")
              .format(sparkTypeToOmniExpJsonType(endsWith.dataType),
                rewriteToOmniJsonExpressionLiteral(endsWith.left, exprsIndexMap),
                generateLikeArg(literal, "endsWith"))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype in like expression: $dataType")
        }
      case contains: Contains =>
        val dataType = contains.right.dataType
        contains.right match {
          case literal: Literal =>
            ("{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
              "\"function_name\":\"LIKE\", \"arguments\":[%s, %s]}")
              .format(sparkTypeToOmniExpJsonType(contains.dataType),
                rewriteToOmniJsonExpressionLiteral(contains.left, exprsIndexMap),
                generateLikeArg(literal, "contains"))
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported datatype in like expression: $dataType")
        }
    }
  }

  def generateLikeArg(literal: Literal, exprFormat: String) : String = {
    val value = literal.value
    if (value == null) {
      return "{\"exprType\":\"LITERAL\",\"dataType\":%s,\"isNull\":%b}".format(sparkTypeToOmniExpJsonType(literal.dataType), true)
    }
    var inputValue = value.toString
    exprFormat match {
      case "startsWith" =>
        inputValue = inputValue + "%"
      case "endsWith" =>
        inputValue = "%" + inputValue
      case "contains" =>
        inputValue = "%" + inputValue + "%"
      case _ =>
        inputValue = value.toString
    }

    val omniType = sparkTypeToOmniExpType(literal.dataType)
    literal.dataType match {
      case StringType =>
        val likeRegExpr = generateLikeRegExpr(inputValue)
        ("{\"exprType\":\"LITERAL\",\"dataType\":%s," +
          "\"isNull\":%b, \"value\":\"%s\",\"width\":%d}")
          .format(omniType, false, likeRegExpr, likeRegExpr.length)
      case dt: DecimalType =>
        toOmniJsonLiteral(literal)
      case _ =>
        toOmniJsonLiteral(literal)
    }
  }

  def generateLikeRegExpr(value : String) : String = {
      val regexString = new mutable.StringBuilder
      regexString.append('^')
      val valueArr =  value.toCharArray
      for (i <- 0 until valueArr.length) {
        valueArr(i) match {
          case '%' =>
            if (i - 1 < 0 || valueArr(i - 1) != '\\') {
              regexString.append(".*")
            } else {
              regexString.append(valueArr(i))
            }

          case '_' =>
            if (i - 1 < 0 || valueArr(i - 1) != '\\') {
              regexString.append(".")
            } else {
              regexString.append(valueArr(i))
            }

          case '\\' =>
            regexString.append("\\")
            regexString.append(valueArr(i))

          case '^' =>
            regexString.append("\\")
            regexString.append(valueArr(i))

          case '$' =>
            regexString.append("\\")
            regexString.append(valueArr(i))

          case '.' =>
            regexString.append("\\")
            regexString.append(valueArr(i))

          case '*' =>
            regexString.append("\\")
            regexString.append(valueArr(i))

          case _ =>
            regexString.append(valueArr(i))

        }
      }
      regexString.append('$')
      regexString.toString()
  }

  def toOmniJoinType(joinType: JoinType): nova.hetu.omniruntime.constants.JoinType = {
    joinType match {
      case FullOuter =>
        OMNI_JOIN_TYPE_FULL
      case _: InnerLike =>
        OMNI_JOIN_TYPE_INNER
      case LeftOuter =>
        OMNI_JOIN_TYPE_LEFT
      case RightOuter =>
        OMNI_JOIN_TYPE_RIGHT
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
}
