package com.huawei.boostkit.spark.expression

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{getExprIdMap, rewriteToOmniExpressionLiteral, rewriteToOmniJsonExpressionLiteral}
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
class OmniExpressionAdaptorSuite extends SparkFunSuite{
  var allAttribute = Seq(AttributeReference("a", IntegerType)(),
    AttributeReference("b", IntegerType)(), AttributeReference("c", BooleanType)(),
    AttributeReference("d", BooleanType)(), AttributeReference("e", IntegerType)(),
    AttributeReference("f", StringType)(), AttributeReference("g", StringType)())

  // todo: CaseWhen,InSet
  test("expression rewrite") {
    checkExpressionRewrite("$operator$ADD:1(#0 , #1)", Add(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$ADD:1(#0 , 1:1)", Add(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$SUBTRACT:1(#0 , #1)",
      Subtract(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$SUBTRACT:1(#0 , 1:1)", Subtract(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$MULTIPLY:1(#0 , #1)",
      Multiply(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$MULTIPLY:1(#0 , 1:1)", Multiply(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$DIVIDE:1(#0 , #1)", Divide(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$DIVIDE:1(#0 , 1:1)", Divide(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$MODULUS:1(#0 , #1)",
      Remainder(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$MODULUS:1(#0 , 1:1)", Remainder(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$GREATER_THAN:4(#0 , #1)",
      GreaterThan(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$GREATER_THAN:4(#0 , 1:1)",
      GreaterThan(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$GREATER_THAN_OR_EQUAL:4(#0 , #1)",
      GreaterThanOrEqual(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$GREATER_THAN_OR_EQUAL:4(#0 , 1:1)",
      GreaterThanOrEqual(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$LESS_THAN:4(#0 , #1)",
      LessThan(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$LESS_THAN:4(#0 , 1:1)",
      LessThan(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$LESS_THAN_OR_EQUAL:4(#0 , #1)",
      LessThanOrEqual(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$LESS_THAN_OR_EQUAL:4(#0 , 1:1)",
      LessThanOrEqual(allAttribute(0), Literal(1)))

    checkExpressionRewrite("$operator$EQUAL:4(#0 , #1)", EqualTo(allAttribute(0), allAttribute(1)))
    checkExpressionRewrite("$operator$EQUAL:4(#0 , 1:1)", EqualTo(allAttribute(0), Literal(1)))

    checkExpressionRewrite("OR:4(#2 , #3)", Or(allAttribute(2), allAttribute(3)))
    checkExpressionRewrite("OR:4(#2 , 3:1)", Or(allAttribute(2), Literal(3)))

    checkExpressionRewrite("AND:4(#2 , #3)", And(allAttribute(2), allAttribute(3)))
    checkExpressionRewrite("AND:4(#2 , 3:1)", And(allAttribute(2), Literal(3)))

    checkExpressionRewrite("not:4(#3)", Not(allAttribute(3)))

    checkExpressionRewrite("IS_NOT_NULL:4(#4)", IsNotNull(allAttribute(4)))

    checkExpressionRewrite("substr:15(#5,#0,#1)",
      Substring(allAttribute(5), allAttribute(1), allAttribute(1)))

    checkExpressionRewrite("CAST:2(#1)", Cast(allAttribute(1), LongType))

    checkExpressionRewrite("abs:1(#0)", Abs(allAttribute(0)))

    checkExpressionRewrite("SUM:2(#0)", Sum(allAttribute(0)))

    checkExpressionRewrite("AVG:3(#0)", Average(allAttribute(0)))

    checkExpressionRewrite("MIN:1(#0)", Min(allAttribute(0)))

    checkExpressionRewrite("IN:4(#0,#0,#1)",
      In(allAttribute(0), Seq(allAttribute(0), allAttribute(1))))
  }

  test("json expression rewrite") {
    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Add(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"ADD\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Add(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Subtract(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"SUBTRACT\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Subtract(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Multiply(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MULTIPLY\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Multiply(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Divide(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"DIVIDE\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Divide(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Remainder(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":1,\"operator\":\"MODULUS\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Remainder(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      GreaterThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      GreaterThanOrEqual(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"GREATER_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      GreaterThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      LessThan(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"LESS_THAN\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      LessThan(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4," +
      "\"operator\":\"LESS_THAN_OR_EQUAL\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      LessThanOrEqual(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      EqualTo(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"EQUAL\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      EqualTo(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      Or(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"OR\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      Or(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      And(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"colVal\":1}}",
      And(allAttribute(0), Literal(1)))

    checkJsonExprRewrite("{\"exprType\":\"BINARY\",\"returnType\":4,\"operator\":\"AND\"," +
      "\"left\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}," +
      "\"right\":{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}}",
      And(allAttribute(0), allAttribute(1)))

    checkJsonExprRewrite("{\"exprType\":\"UNARY\",\"returnType\":4,\"operator\":\"not\"," +
      "\"expr\":{\"exprType\":\"IS_NULL\",\"returnType\":4," +
      "\"arguments\":[{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":4}]}}",
      IsNotNull(allAttribute(4)))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":2,\"function_name\":\"CAST\"," +
      "\"arguments\":[{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":1}]}}",
      Cast(allAttribute(1), LongType))

    checkJsonExprRewrite("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"abs\"," +
      "\"arguments\":[{\"exprType\":\"FILED_REFERENCE\",\"dataType\":1,\"colVal\":0}]}}",
      Abs(allAttribute(0)))
  }

  private def checkExpressionRewrite(expected: Any, expression: Expression): Unit = {
    {
      val runResult = rewriteToOmniExpressionLiteral(expression, getExprIdMap(allAttribute))
      if (!expected.equals(runResult)) {
        fail(s"expression($expression) not match with expected value:$expected," +
          s"running value:$runResult")
      }
    }
  }

  private def checkExpressionRewrite(expected: Any, expression: Expression): Unit = {
    {
      val runResult = rewriteToOmniJsonExpressionLiteral(expression, getExprIdMap(allAttribute))
      if (!expected.equals(runResult)) {
        fail(s"expression($expression) not match with expected value:$expected," +
          s"running value:$runResult")
      }
    }
  }
}
