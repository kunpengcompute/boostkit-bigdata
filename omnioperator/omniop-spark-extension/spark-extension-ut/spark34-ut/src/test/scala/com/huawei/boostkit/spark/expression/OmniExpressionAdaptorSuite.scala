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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{getExprIdMap, procCaseWhenExpression, rewriteToOmniJsonExpressionLiteral}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
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

  test("json expression rewrite") {
    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Add(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      Add(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Subtract(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      Subtract(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Multiply(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      Multiply(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Divide(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      Divide(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Remainder(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      Remainder(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      GreaterThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThanOrEqual(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      GreaterThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      LessThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThanOrEqual(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      LessThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      EqualTo(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":1}}",
      EqualTo(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":3}}",
      Or(allAttribute(2), allAttribute(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":3}}",
      Or(allAttribute(2), Literal(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":3}}",
      And(allAttribute(2), allAttribute(3)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":4,\"colVal\":2}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":3}}",
      And(allAttribute(2), Literal(3)))

    checkJsonExprRewrite("{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\"," +
      "\"expr\":{\"exprType\":\"IS_NULL\",\"returnType\":4," +
      "\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4}]}}",
      IsNotNull(allAttribute(4)))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":2,\"function_name\":\"CAST\"," +
      "\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":1}]}",
      Cast(allAttribute(1), LongType))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"abs\"," +
      " \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0}]}",
      Abs(allAttribute(0)))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"round\"," +
      " \"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":0},{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":2}]}",
      Round(allAttribute(0), Literal(2)))
  }

  protected def checkJsonExprRewrite(expected: Any, expression: Expression): Unit = {
    val runResult = rewriteToOmniJsonExpressionLiteral(expression, getExprIdMap(allAttribute))
    checkJsonKeyValueIgnoreKeySequence(expected.asInstanceOf[String], runResult, expression)
  }

  private def checkJsonKeyValueIgnoreKeySequence(expected: String, runResult: String, expression: Expression) : Unit = {
    // 将expected runResult 两个json字符串中的key排序后比较两个json字符串是否相同
    val objectMapper = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
    val expectedJsonNode = objectMapper.readTree(expected)
    val runResultJsonNode = objectMapper.readTree(runResult)
    val expectedIgnoreKeySequence = objectMapper.writeValueAsString(objectMapper.treeToValue(expectedJsonNode, classOf[Object]))
    val runResultIgnoreKeySequence = objectMapper.writeValueAsString(objectMapper.treeToValue(runResultJsonNode, classOf[Object]))
    if (!expectedIgnoreKeySequence.equals(runResultIgnoreKeySequence)) {
      fail(s"expression($expression) not match with expected value:$expectedIgnoreKeySequence," +
        s"running value:$runResultIgnoreKeySequence")
    }
  }

  test("json expression rewrite support Chinese") {
    val cnAttribute = Seq(AttributeReference("char_1", StringType)(), AttributeReference("char_20", StringType)(),
      AttributeReference("varchar_1", StringType)(), AttributeReference("varchar_20", StringType)())

    val t1 = new Tuple2(Not(EqualTo(cnAttribute(0), Literal("新"))), Not(EqualTo(cnAttribute(1), Literal("官方爸爸"))))
    val t2 = new Tuple2(Not(EqualTo(cnAttribute(2), Literal("爱你三千遍"))), Not(EqualTo(cnAttribute(2), Literal("新"))))
    val branch = Seq(t1, t2)
    val elseValue = Some(Not(EqualTo(cnAttribute(3), Literal("啊水水水水"))))
    val caseWhen = CaseWhen(branch, elseValue);
    val caseWhenResult = rewriteToOmniJsonExpressionLiteral(caseWhen, getExprIdMap(cnAttribute))
    val caseWhenExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"新\",\"width\":1}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"官方爸爸\",\"width\":4}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"爱你三千遍\",\"width\":5}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"新\",\"width\":1}},\"if_false\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":3,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"啊水水水水\",\"width\":5}}}}"

    checkJsonKeyValueIgnoreKeySequence(caseWhenExp, caseWhenResult, caseWhen)

    val isNull = IsNull(cnAttribute(0));
    val isNullResult = rewriteToOmniJsonExpressionLiteral(isNull, getExprIdMap(cnAttribute))
    val isNullExp = "{\"exprType\":\"IS_NULL\",\"returnType\":4,\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50}]}"

    checkJsonKeyValueIgnoreKeySequence(isNullExp, isNullResult, isNull)

    val children = Seq(cnAttribute(0), cnAttribute(1))
    val coalesce = Coalesce(children);
    val coalesceResult = rewriteToOmniJsonExpressionLiteral(coalesce, getExprIdMap(cnAttribute))
    val coalesceExp = "{\"exprType\":\"COALESCE\",\"returnType\":15,\"width\":50,\"value1\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50},\"value2\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":50}}"

    checkJsonKeyValueIgnoreKeySequence(coalesceExp, coalesceResult, coalesce)

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
    val runResult = procCaseWhenExpression(expression, getExprIdMap(caseWhenAttribute)).toString()
    val filterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"新\",\"width\":1}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"官方爸爸\",\"width\":4}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"爱你三千遍\",\"width\":5}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":2,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"新\",\"width\":1}},\"if_false\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":3,\"width\":50},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":15,\"isNull\":false,\"value\":\"啊水水水水\",\"width\":5}}}}"

    checkJsonKeyValueIgnoreKeySequence(filterExp, runResult, expression)

    val t3 = new Tuple2(Not(EqualTo(caseWhenAttribute(4), Literal(5))), Not(EqualTo(caseWhenAttribute(5), Literal(10))))
    val t4 = new Tuple2(LessThan(caseWhenAttribute(4), Literal(15)), GreaterThan(caseWhenAttribute(5), Literal(20)))
    val branch2 = Seq(t3, t4)
    val elseValue2 = Some(Not(EqualTo(caseWhenAttribute(5), Literal(25))))
    val numExpression = CaseWhen(branch2, elseValue2);
    val numResult = procCaseWhenExpression(numExpression, getExprIdMap(caseWhenAttribute)).toString()
    val numFilterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":5}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":10}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":15}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":20}},\"if_false\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":25}}}}"

    checkJsonKeyValueIgnoreKeySequence(numFilterExp, numResult, numExpression)

    val t5 = new Tuple2(Not(EqualTo(caseWhenAttribute(4), Literal(5))), Not(EqualTo(caseWhenAttribute(5), Literal(10))))
    val t6 = new Tuple2(LessThan(caseWhenAttribute(4), Literal(15)), GreaterThan(caseWhenAttribute(5), Literal(20)))
    val branch3 = Seq(t5, t6)
    val elseValue3 = None
    val noneExpression = CaseWhen(branch3, elseValue3);
    val noneResult = procCaseWhenExpression(noneExpression, getExprIdMap(caseWhenAttribute)).toString()
    val noneFilterExp = "{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":5}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"NOT_EQUAL\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":10}},\"if_false\":{\"exprType\":\"IF\",\"returnType\":4,\"condition\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":4},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":15}},\"if_true\":{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"GREATER_THAN\",\"left\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":1,\"colVal\":5},\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":20}},\"if_false\":{\"exprType\":\"LITERAL\",\"dataType\":4,\"isNull\":true}}}"

    checkJsonKeyValueIgnoreKeySequence(noneFilterExp, noneResult, noneExpression)

    val t7 = Tuple2(Not(EqualTo(caseWhenAttribute(0), Literal("\"\\\\t/\\b\\f\\n\\r\\t123"))), Not(EqualTo(caseWhenAttribute(1), Literal("\"\\\\t/\\b\\f\\n\\r\\t234"))))
    val t8 = Tuple2(Not(EqualTo(caseWhenAttribute(2), Literal("\"\\\\t/\\b\\f\\n\\r\\t345"))), Not(EqualTo(caseWhenAttribute(2), Literal("\"\\\\t/\\b\\f\\n\\r\\t123"))))
    val branch4 = Seq(t7, t8)
    val elseValue4 = Some(Not(EqualTo(caseWhenAttribute(3), Literal("\"\\\\t/\\b\\f\\n\\r\\t456"))))
    val specialCharacterExpression = CaseWhen(branch4, elseValue4);
    val specialCharacterRunResult = procCaseWhenExpression(specialCharacterExpression, getExprIdMap(caseWhenAttribute)).toString()
    val specialCharacterFilterExp = "{\"condition\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":0,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t123\",\"width\":18}},\"exprType\":\"IF\",\"if_false\":{\"condition\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":2,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t345\",\"width\":18}},\"exprType\":\"IF\",\"if_false\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":3,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t456\",\"width\":18}},\"if_true\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":2,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t123\",\"width\":18}},\"returnType\":4},\"if_true\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":1,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t234\",\"width\":18}},\"returnType\":4} "

    checkJsonKeyValueIgnoreKeySequence(specialCharacterFilterExp, specialCharacterRunResult, specialCharacterExpression)

  }

  test("test special character rewrite") {
    val specialCharacterAttribute = Seq(AttributeReference("char_1", StringType)(), AttributeReference("char_20", StringType)(),
      AttributeReference("varchar_1", StringType)(), AttributeReference("varchar_20", StringType)())

    val t1 = new Tuple2(Not(EqualTo(specialCharacterAttribute(0), Literal("\"\\\\t/\\b\\f\\n\\r\\t123"))), Not(EqualTo(specialCharacterAttribute(1), Literal("\"\\\\t/\\b\\f\\n\\r\\t234"))))
    val t2 = new Tuple2(Not(EqualTo(specialCharacterAttribute(2), Literal("\"\\\\t/\\b\\f\\n\\r\\t345"))), Not(EqualTo(specialCharacterAttribute(2), Literal("\"\\\\t/\\b\\f\\n\\r\\t456"))))
    val branch = Seq(t1, t2)
    val elseValue = Some(Not(EqualTo(specialCharacterAttribute(3), Literal("\"\\\\t/\\b\\f\\n\\r\\t456"))))
    val caseWhen = CaseWhen(branch, elseValue);
    val caseWhenResult = rewriteToOmniJsonExpressionLiteral(caseWhen, getExprIdMap(specialCharacterAttribute))
    val caseWhenExp = "{\"condition\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":0,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t123\",\"width\":18}},\"exprType\":\"IF\",\"if_false\":{\"condition\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":2,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t345\",\"width\":18}},\"exprType\":\"IF\",\"if_false\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":3,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t456\",\"width\":18}},\"if_true\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":2,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t456\",\"width\":18}},\"returnType\":4},\"if_true\":{\"exprType\":\"BINARY\",\"left\":{\"colVal\":1,\"dataType\":15,\"exprType\":\"FIELD_REFERENCE\",\"width\":50},\"operator\":\"NOT_EQUAL\",\"returnType\":4,\"right\":{\"dataType\":15,\"exprType\":\"LITERAL\",\"isNull\":false,\"value\":\"\\\"\\\\\\\\t/\\\\b\\\\f\\\\n\\\\r\\\\t234\",\"width\":18}},\"returnType\":4}"
    checkJsonKeyValueIgnoreKeySequence(caseWhenExp, caseWhenResult, caseWhen)

    val isNull = IsNull(specialCharacterAttribute(0));
    val isNullResult = rewriteToOmniJsonExpressionLiteral(isNull, getExprIdMap(specialCharacterAttribute))
    val isNullExp = "{\"exprType\":\"IS_NULL\",\"returnType\":4,\"arguments\":[{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50}]}"

    checkJsonKeyValueIgnoreKeySequence(isNullExp, isNullResult, isNull)

    val children = Seq(specialCharacterAttribute(0), specialCharacterAttribute(1))
    val coalesce = Coalesce(children);
    val coalesceResult = rewriteToOmniJsonExpressionLiteral(coalesce, getExprIdMap(specialCharacterAttribute))
    val coalesceExp = "{\"exprType\":\"COALESCE\",\"returnType\":15,\"width\":50,\"value1\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":0,\"width\":50},\"value2\":{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":15,\"colVal\":1,\"width\":50}}"
    checkJsonKeyValueIgnoreKeySequence(coalesceExp, coalesceResult, coalesce)
  }

  test("test double -0.0") {
    val literal = Literal(-0.0d)
    val result = rewriteToOmniJsonExpressionLiteral(literal, Map.empty)
    val expected = "{\"exprType\":\"LITERAL\",\"dataType\":3,\"isNull\":false,\"value\":-0.0}"
    checkJsonKeyValueIgnoreKeySequence(expected, result, literal)
  }

}
