/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{getExprIdMap, procCaseWhenExpression, procLikeExpression, rewriteToOmniExpressionLiteral, rewriteToOmniJsonExpressionLiteral}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Max, Min, Sum}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType}

/**
 * 功能描述
 *
 * @author w00630100
 * @since 2022-02-21
 */
class OmniExpressionAdaptorSuite extends SparkFunSuite {
  var allAttribute = Seq(AttributeReference("a", IntegerType)(),
    AttributeReference("b", IntegerType)(), AttributeReference("c", BooleanType)(),
    AttributeReference("d", BooleanType)(), AttributeReference("e", IntegerType)(),
    AttributeReference("f", StringType)(), AttributeReference("g", StringType)())

  // todo: CaseWhen,InSet
  test("expression rewrite") {
    checkExpressionRewrite("$operator$ADD:1(#0,#1)", Add(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$ADD:1(#0,1:1)", Add(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$SUBTRACT:1(#0,#1)",
      Subtract(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$SUBTRACT:1(#0,1:1)", Subtract(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$MULTIPLY:1(#0,#1)",
      Multiply(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$MULTIPLY:1(#0,1:1)", Multiply(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$DIVIDE:1(#0,#1)", Divide(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$DIVIDE:1(#0,1:1)", Divide(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$MODULUS:1(#0,#1)",
      Remainder(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$MODULUS:1(#0,1:1)", Remainder(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$GREATER_THAN:4(#0,#1)",
      GreaterThan(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$GREATER_THAN:4(#0,1:1)",
      GreaterThan(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$GREATER_THAN_OR_EQUAL:4(#0,#1)",
      GreaterThanOrEqual(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$GREATER_THAN_OR_EQUAL:4(#0,1:1)",
      GreaterThanOrEqual(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$LESS_THAN:4(#0,#1)",
      LessThan(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$LESS_THAN:4(#0,1:1)",
      LessThan(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$LESS_THAN_OR_EQUAL:4(#0,#1)",
      LessThanOrEqual(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$LESS_THAN_OR_EQUAL:4(#0,1:1)",
      LessThanOrEqual(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$EQUAL:4(#0,#1)", EqualTo(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$EQUAL:4(#0,1:1)", EqualTo(allAttribute(0), Literal(1)))

    checkExpressionRewrite("OR:4(#2,#3)", Or(allAttribute(2), allAttribute(3)))
    checkExpressionRewrite("OR:4(#2,3:1)", Or(allAttribute(2), Literal(3)))

    checkExpressionRewrite("AND:4(#2,#3)", And(allAttribute(2), allAttribute(3)))
    checkExpressionRewrite("AND:4(#2,3:1)", And(allAttribute(2), Literal(3)))

    checkExpressionRewrite("not:4(#3)", Not(allAttribute(3)))

    checkExpressionRewrite("IS_NOT_NULL:4(#4)", IsNotNull(allAttribute(4)))

    checkExpressionRewrite("substr:15(#5,#0,#1)",
      Substring(allAttribute(5), allAttribute(0), allAttribute(1)))

    checkExpressionRewrite("CAST:2(#1)", Cast(allAttribute(1), LongType))

    checkExpressionRewrite("abs:1(#0)", Abs(allAttribute(0)))

    checkExpressionRewrite("SUM:2(#0)", Sum(allAttribute(0)))

    checkExpressionRewrite("MAX:1(#0)", Max(allAttribute(0)))

    checkExpressionRewrite("AVG:3(#0)", Average(allAttribute(0)))

    checkExpressionRewrite("MIN:1(#0)", Min(allAttribute(0)))

    checkExpressionRewrite("IN:4(#0,#0,#1)",
      In(allAttribute(0), Seq(allAttribute(0), allAttribute(1))))

    //    checkExpressionRewrite("IN:4(#0, #0, #1)", InSet(allAttribute(0), Set(allAttribute(0), allAttribute(1))))
  }

  test("json expression rewrite") {
    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Add(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      Add(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Subtract(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      Subtract(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Multiply(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      Multiply(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Divide(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      Divide(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Remainder(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      Remainder(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      GreaterThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThanOrEqual(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      GreaterThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      LessThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThanOrEqual(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      LessThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      EqualTo(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":1}}",
      EqualTo(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":3}}",
      Or(allAttribute(2), allAttribute(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":3}}",
      Or(allAttribute(2), Literal(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":3}}",
      And(allAttribute(2), allAttribute(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":3}}",
      And(allAttribute(2), Literal(3)))

    checkJsonExprRewrite("{\"exprType\":\"UNARY\",\"returnType\":4, \"operator\":\"not\"," +
      "\"expr\":{\"exprType\":\"IS_NULL\",\"returnType\":4," +
      "\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4}]}}",
      IsNotNull(allAttribute(4)))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":2,\"function_name\":\"CAST\"," +
      "\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}]}",
      Cast(allAttribute(1), LongType))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"abs\"," +
      " \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}]}",
      Abs(allAttribute(0)))
  }

  protected def checkExpressionRewrite(expected: Any, expression: Expression): Unit = {
    {
      val runResult = rewriteToOmniExpressionLiteral(expression, getExprIdMap(allAttribute))
      if (!expected.equals(runResult)) {
        fail(s"expression($expression) not match with expected value:$expected," +
          s"running value:$runResult")
      }
    }
  }

  protected def checkJsonExprRewrite(expected: Any, expression: Expression): Unit = {
    val runResult = rewriteToOmniJsonExpressionLiteral(expression, getExprIdMap(allAttribute))
    if (!expected.equals(runResult)) {
      fail(s"expression($expression) not match with expected value:$expected," +
        s"running value:$runResult")
    }
  }

  test("json expression rewrite support Chinese") {
    val cnAttribute = Seq(AttributeReference("char_1", StringType)(), AttributeReference("char_20", StringType)(),
      AttributeReference("varchar_1", StringType)(), AttributeReference("varchar_20", StringType)())

    val like = Like(cnAttribute(2), Literal("我_"), '\\');
    val likeResult = procLikeExpression(like, getExprIdMap(cnAttribute))
    val likeExp = "{\"exprType\":\"FUNCTION\",\"returnType\":4,\"function_name\":\"LIKE\", \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000}, {\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"^我.$\",\"width\":4}]}"
    if (!likeExp.equals(likeResult)) {
      fail(s"expression($like) not match with expected value:$likeExp," +
        s"running value:$likeResult")
    }

    val startsWith = StartsWith(cnAttribute(2), Literal("我"));
    val startsWithResult = procLikeExpression(startsWith, getExprIdMap(cnAttribute))
    val startsWithExp = "{\"exprType\":\"FUNCTION\",\"returnType\":4,\"function_name\":\"LIKE\", \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000}, {\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"^我.*$\",\"width\":5}]}"
    if (!startsWithExp.equals(startsWithResult)) {
      fail(s"expression($startsWith) not match with expected value:$startsWithExp," +
        s"running value:$startsWithResult")
    }

    val endsWith = EndsWith(cnAttribute(2), Literal("我"));
    val endsWithResult = procLikeExpression(endsWith, getExprIdMap(cnAttribute))
    val endsWithExp = "{\"exprType\":\"FUNCTION\",\"returnType\":4,\"function_name\":\"LIKE\", \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000}, {\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"^.*我$\",\"width\":5}]}"
    if (!endsWithExp.equals(endsWithResult)) {
      fail(s"expression($endsWith) not match with expected value:$endsWithExp," +
        s"running value:$endsWithResult")
    }

    val contains = Contains(cnAttribute(2), Literal("我"));
    val containsResult = procLikeExpression(contains, getExprIdMap(cnAttribute))
    val containsExp = "{\"exprType\":\"FUNCTION\",\"returnType\":4,\"function_name\":\"LIKE\", \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000}, {\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"^.*我.*$\",\"width\":7}]}"
    if (!containsExp.equals(containsResult)) {
      fail(s"expression($contains) not match with expected value:$containsExp," +
        s"running value:$containsResult")
    }

    val t1 = new Tuple2(Not(EqualTo(cnAttribute(0), Literal("新"))), Not(EqualTo(cnAttribute(1), Literal("官方爸爸"))))
    val t2 = new Tuple2(Not(EqualTo(cnAttribute(2), Literal("爱你三千遍"))), Not(EqualTo(cnAttribute(2), Literal("新"))))
    val branch = Seq(t1, t2)
    val elseValue = Some(Not(EqualTo(cnAttribute(3), Literal("啊水水水水"))))
    val caseWhen = CaseWhen(branch, elseValue);
    val caseWhenResult = rewriteToOmniJsonExpressionLiteral(caseWhen, getExprIdMap(cnAttribute))
    val caseWhenExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"新\",\"width\":1}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"官方爸爸\",\"width\":4}}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"爱你三千遍\",\"width\":5}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"新\",\"width\":1}}},\"if_false\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":3,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"啊水水水水\",\"width\":5}}}}}"
    if (!caseWhenExp.equals(caseWhenResult)) {
      fail(s"expression($caseWhen) not match with expected value:$caseWhenExp," +
        s"running value:$caseWhenResult")
    }

    val isNull = IsNull(cnAttribute(0));
    val isNullResult = rewriteToOmniJsonExpressionLiteral(isNull, getExprIdMap(cnAttribute))
    val isNullExp = "{\"exprType\":\"IS_NULL\",\"returnType\":4,\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":2000}]}"
    if (!isNullExp.equals(isNullResult)) {
      fail(s"expression($isNull) not match with expected value:$isNullExp," +
        s"running value:$isNullResult")
    }

    val children = Seq(cnAttribute(0), cnAttribute(1))
    val coalesce = Coalesce(children);
    val coalesceResult = rewriteToOmniJsonExpressionLiteral(coalesce, getExprIdMap(cnAttribute))
    val coalesceExp = "{\"exprType\":\"COALESCE\",\"returnType\":15,\"width\":2000, \"value1\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":2000},\"value2\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":2000}}"
    if (!coalesceExp.equals(coalesceResult)) {
      fail(s"expression($coalesce) not match with expected value:$coalesceExp," +
        s"running value:$coalesceResult")
    }

    val children2 = Seq(cnAttribute(0), cnAttribute(1), cnAttribute(2))
    val coalesce2 = Coalesce(children2);
    try {
        rewriteToOmniJsonExpressionLiteral(coalesce2, getExprIdMap(cnAttribute))
    } catch {
      case ex: UnsupportedOperationException => {
         println(ex)
      }
    }

  }

  test("procCaseWhenExpression") {
    val caseWhenAttribute = Seq(AttributeReference("char_1", StringType)(), AttributeReference("char_20", StringType)(),
      AttributeReference("varchar_1", StringType)(), AttributeReference("varchar_20", StringType)(),
      AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)())

    val t1 = new Tuple2(Not(EqualTo(caseWhenAttribute(0), Literal("新"))), Not(EqualTo(caseWhenAttribute(1), Literal("官方爸爸"))))
    val t2 = new Tuple2(Not(EqualTo(caseWhenAttribute(2), Literal("爱你三千遍"))), Not(EqualTo(caseWhenAttribute(2), Literal("新"))))
    val branch = Seq(t1, t2)
    val elseValue = Some(Not(EqualTo(caseWhenAttribute(3), Literal("啊水水水水"))))
    val expression = CaseWhen(branch, elseValue);
    val runResult = procCaseWhenExpression(expression, getExprIdMap(caseWhenAttribute))
    val filterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"新\",\"width\":1}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"官方爸爸\",\"width\":4}}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"爱你三千遍\",\"width\":5}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"新\",\"width\":1}}},\"if_false\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":3,\"width\":2000},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false, \"value\":\"啊水水水水\",\"width\":5}}}}}"
    if (!filterExp.equals(runResult)) {
      fail(s"expression($expression) not match with expected value:$filterExp," +
        s"running value:$runResult")
    }

    val t3 = new Tuple2(Not(EqualTo(caseWhenAttribute(4), Literal(5))), Not(EqualTo(caseWhenAttribute(5), Literal(10))))
    val t4 = new Tuple2(LessThan(caseWhenAttribute(4), Literal(15)), GreaterThan(caseWhenAttribute(5), Literal(20)))
    val branch2 = Seq(t3, t4)
    val elseValue2 = Some(Not(EqualTo(caseWhenAttribute(5), Literal(25))))
    val numExpression = CaseWhen(branch2, elseValue2);
    val numResult = procCaseWhenExpression(numExpression, getExprIdMap(caseWhenAttribute))
    val numFilterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":5}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":10}}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":15}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":20}},\"if_false\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":25}}}}}"
    if (!numFilterExp.equals(numResult)) {
      fail(s"expression($numExpression) not match with expected value:$numFilterExp," +
        s"running value:$numResult")
    }

    val t5 = new Tuple2(Not(EqualTo(caseWhenAttribute(4), Literal(5))), Not(EqualTo(caseWhenAttribute(5), Literal(10))))
    val t6 = new Tuple2(LessThan(caseWhenAttribute(4), Literal(15)), GreaterThan(caseWhenAttribute(5), Literal(20)))
    val branch3 = Seq(t5, t6)
    val elseValue3 = None
    val noneExpression = CaseWhen(branch3, elseValue3);
    val noneResult = procCaseWhenExpression(noneExpression, getExprIdMap(caseWhenAttribute))
    val noneFilterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":5}}},\"if_true\":{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\",\"expr\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":10}}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":15}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1, \"isNull\":false, \"value\":20}},\"if_false\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":true}}}"
    if (!noneFilterExp.equals(noneResult)) {
      fail(s"expression($noneExpression) not match with expected value:$noneFilterExp," +
        s"running value:$noneResult")
    }
  }


}
