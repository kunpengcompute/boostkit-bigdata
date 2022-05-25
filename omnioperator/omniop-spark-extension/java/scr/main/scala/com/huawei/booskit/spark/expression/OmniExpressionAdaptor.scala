package scr.main.scala.com.huawei.booskit.spark.expression

import scr.main.scala.com.huawei.booskit.spark.Constant.{DEFAULT_STRING_TYPE_LENGTH, OMNI_BOOLEAN_TYPE, OMNI_DATE_TYPE, OMNI_DECIMAL128_TYPE, OMNI_DECIMAL64_TYPE, OMNI_DOUBLE_TYPE, OMNI_INTEGER_TYPE, OMNI_LONG_TYPE, OMNI_SHORT_TYPE, OMNI_VARCHAR_TYPE}

object OmniExpressionAdaptor extends Logging {

  def getRealExprId(expr: Expression): ExprId = {
    // TODO support more complex expression
    expr match {
      case alias: Alias => getRealExprId(alias.child)
      case subString: Substring => getRealExprId(subString.str)
      case attr: Attribute => attr.exprId
      case _ =>
        throw new RuntimeException(s"Unsupported expression: $expr")
    }
  }

  def getExprIdMap(inputAttrs: Seq[Attribute]): Map[ExprId, Int] = {
    var attrMap: Map[ExprId, Int] = Map()
    inputAttrs.zipWithIndex.foreach { case (inputAttr, i) =>
      attrMap += (inputAttr.exprId -> i)
    }
    attrMap
  }

  def rewriteToOmniExpressionLiteral(expr: Expression, expresIndexMap: Map[ExprId, Int]): String = {
    expr match {
      case unscaledValue: UnscaledValue =>
        "UnscaledValue:%s(%s, %d, %d)".format(
          sparkTypeToOmniExpType(unscaledValue.dataType),
          rewriteToOmniExpressionLiteral(unscaledValue.child, expresIndexMap),
          unscaledValue.child.dataType.asInstanceOf[DecimalType].precision,
          unscaledValue.child.dataType.asInstanceOf[DecimalType].scale)

      case checkOverflow: CheckOverflow =>
        "IF:%s(IsOverflowDecimal:%s(%s,%d,%d,%d,%d), %s, MakeDecimal:%s(%s,%d,%d,%d,%d))"
          .format(sparkTypeToOmniExpType(checkOverflow.dataType),
            // IsOverflowDecimal returnType
            sparkTypeToOmniExpType(BooleanType),
            // IsOverflowDecimal arguments
            rewriteToOmniExpressionLiteral(checkOverflow.child, expresIndexMap),
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            // if_true
            rewriteToOmniExpressionLiteral(Literal(null, checkOverflow.dataType), expresIndexMap),
            sparkTypeToOmniExpType(checkOverflow.dataType),
            rewriteToOmniExpressionLiteral(checkOverflow.child, expresIndexMap),
            checkOverflow.dataType.precision, checkOverflow.dataType.scale,
            checkOverflow.dataType.precision, checkOverflow.dataType.scale)

      case makeDecimal: MakeDecimal =>
        makeDecimal.child.dataType match {
          case decimalChild: DecimalType =>
            "MakeDecimal:%s(%s,%s,%s,%s,%s)"
              .format(sparkTypeToOmniExpType(makeDecimal.dataType),
                rewriteToOmniExpressionLiteral(makeDecimal.child, exprsIndexMap),
                toOmniLiteral(Literal(decimalChild.precision, IntegerType)),
                toOmniLiteral(Literal(decimalChild.scale, IntegerType)),
                toOmniLiteral(Literal(decimalChild.precision, IntegerType)),
                toOmniLiteral(Literal(decimalChild.scale, IntegerType)))

          case longChild: LongType =>
            "MakeDecimal:%s(%s,%s,%s)".format(sparkTypeToOmniExpType(makeDecimal.dataType),
              rewriteToOmniExpressionLiteral(makeDecimal.child, exprsIndexMap),
              makeDecimal.precision,
              makeDecimal.scale)
          case _ =>
            throw new RuntimeException(s"Unsupported dataType for MakeDecimal: ${makeDecimal.child.dataType}")
        }

      case promotePrecision: PromotePrecision =>
        rewriteToOmniJsonExpressionLiteral(promotePrecision.child, expresIndexMap)

      case sub: SubStract =>
        "$operator$SUBTRACT:%s(%s,%s)".format(sparkTypeToOmniExpType(sub.dataType),
          rewriteToOmniExpressionLiteral(sub.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(sub.right, exprsIndexMap))

      case add: Add =>
        "$operator$ADD:%s(%s,%s)".format(sparkTypeToOmniExpType(add.dataType),
          rewriteToOmniExpressionLiteral(add.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(add.right, exprsIndexMap))

      case mult: Multiply =>
        "$operator$MULTIPLY:%s(%s,%s)".format(sparkTypeToOmniExpType(mult.dataType),
          rewriteToOmniExpressionLiteral(mult.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(mult.right, exprsIndexMap))

      case divide: Divide =>
        "$operator$DIVIDE:%s(%s,%s)".format(sparkTypeToOmniExpType(divide.dataType),
          rewriteToOmniExpressionLiteral(divide.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(divide.right, exprsIndexMap))

      case mod: Remainder =>
        "$operator$mod:%s(%s,%s)".format(sparkTypeToOmniExpType(mod.dataType),
          rewriteToOmniExpressionLiteral(mod.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(mod.right, exprsIndexMap))

      case greaterThan: GreaterThan =>
        "$operator$GREATER_THAN:%s(%s,%s)".format(sparkTypeToOmniExpType(greaterThan.dataType),
          rewriteToOmniExpressionLiteral(greaterThan.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(greaterThan.right, exprsIndexMap))

      case greaterThanOrEq: GreaterThanOrEqual =>
        "$operator$GREATER_THAN_OR_EQUAL:%s(%s,%s)".format(sparkTypeToOmniExpType(greaterThanOrEq.dataType),
          rewriteToOmniExpressionLiteral(greaterThanOrEq.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(greaterThanOrEq.right, exprsIndexMap))

      case lessThan: LessThan =>
        "$operator$LESS_THAN:%s(%s,%s)".format(sparkTypeToOmniExpType(lessThan.dataType),
          rewriteToOmniExpressionLiteral(lessThan.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(lessThan.right, exprsIndexMap))

      case lessThanOrEq: LessThanOrEqual =>
        "$operator$LESS_THAN_OR_EQUAL:%s(%s,%s)".format(sparkTypeToOmniExpType(lessThanOrEq.dataType),
          rewriteToOmniExpressionLiteral(lessThanOrEq.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(lessThanOrEq.right, exprsIndexMap))

      case equal: EqualTo =>
        "$operator$EQUAL:%s(%s,%s)".format(sparkTypeToOmniExpType(equal.dataType),
          rewriteToOmniExpressionLiteral(equal.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(equal.right, exprsIndexMap))

      case or: Or =>
        "$operator$OR:%s(%s,%s)".format(sparkTypeToOmniExpType(or.dataType),
          rewriteToOmniExpressionLiteral(or.left, exprsIndexMap),
          rewriteToOmniExpressionLiteral(or.right, exprsIndexMap))

      case and: And =>
        "$operator$AND:%s(%s,%s)".format(sparkTypeToOmniExpType(and.dataType),
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

      case subString: SubString =>
        "substr:%s(%s,%s,%s)".format(
          sparkTypeToOmniExpType(subString.dataType),
          rewriteToOmniExpressionLiteral(subString.str, exprsIndexMap),
          rewriteToOmniExpressionLiteral(subString.pos, exprsIndexMap),
          rewriteToOmniExpressionLiteral(subString.len, exprsIndexMap))

      case cast: Cast =>
        unsupportedCastCheck(expr, cast)
        "CAST:%s(%s)".format(
          sparkTypeToOmniExpType(cast.dataType),
          rewriteToOmniExpressionLiteral(cast.child, expresIndexMap))

      case abs: Abs =>
        "abs:%s(%s)".format(
          sparkTypeToOmniExpType(abs.dataType),
          rewriteToOmniExpressionLiteral(abs.child, expresIndexMap))

      case in: In =>
        "IN:%s(%s)".format(
          sparkTypeToOmniExpType(in.dataType),
          in.map(child => rewriteToOmniExpressionLiteral(child, expresIndexMap)).mkString("[", ",", "]"))

      case inset: InSet =>
        "IN:%s(%s,%s)".format(
          sparkTypeToOmniExpType(inset.dataType),
          rewriteToOmniExpressionLiteral(inset.child, expresIndexMap),
          inset.set.map(child => toOmniLiteral(Literal(child, inset.child.dataType))).mkString("[", ",", "]"))

      case caseWhen: CaseWhen =>
        "IF:%s(%s,%s,%s)".format(
          sparkTypeToOmniExpType(ifExp.dataType),
          rewriteToOmniExpressionLiteral(ifExp.predicate, exprsIndexMap),
          rewriteToOmniExpressionLiteral(ifExp.trueValue, exprsIndexMap),
          rewriteToOmniExpressionLiteral(ifExp.falseValue, exprsIndexMap))

      case coalesce: Coalesce =>
        "COALESCE:%s(%s)".format(
          sparkTypeToOmniExpType(coalesce.dataType),
          coalesce.children.map(child => rewriteToOmniExpressionLiteral(child, expresIndexMap)).mkString(","))

      case concat: Concat =>
        getConcatStr(concat, exprsIndexMap)
      case attr: Attribute => toOmniJsonAttribute(attr, exprsIndexMap(attr.exprId))
      case unscaledValue: UnscaledValue =>
        rewriteToOmniExpressionLiteral(unscaledValue.child, expresIndexMap)
      case _ =>
        throw new RuntimeException(s"Unsupported expression: $expr")
    }
  }

  private def getConcatStr(concat: Concat, exprsIndexMap: Map[ExprId, Int]): String = {
    val children: Seq[Expression] = concat.children
    checkInputDataTypes(children)
    val template = "concat:%s(%s,%s)"
    val returnType = sparkTypeToOmniExpJsonType(concat.dataType)
    if (children.length == 1) {
      return rewriteToOmniExpressionLiteral(children.head, exprsIndexMap)
    }
    // (a, b, c) => concat(concat(a,b),c)
    var res = template.format(returnType,
      rewriteToOmniExpressionLiteral(children.head, exprsIndexMap),
      rewriteToOmniExpressionLiteral(children(1), exprsIndexMap))
    for (i <- 2 until children.length) {
      res = template.format(returnType, res,
        rewriteToOmniExpressionLiteral(children(i), exprsIndexMap))
    }
    res
  }

  private def unsupportedCastCheck(expr: Expression, cast: Cast): Unit = {
    if (cast.dataType == StringType && cast.child.dataType != StringType) {
      throw new RuntimeException(s"Unsupported expression: $expr")
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

  def rewriteToOmniJsonExpressionLiteral(expr: Expression, exprsIndexMap: Map[ExprId, Int]): String = {
    rewriteToOmniJsonExpressionLiteral(expr, exprsIndexMap, expr.dataType)
  }

  def rewriteToOmniJsonExpressionLiteral(expr: Expression, exprsIndexMap: Map[ExprId, Int], returnDataType: DataType): String = {
    expr match {
      case unscaledValue: Unscaledvalue =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"function_name\":\"Unscaledvalue\", \"arguments\":[%s, %s, %s]}"
          .format(sparkTypeToOmniExpJsonType(unscaledValue.dataType), rewriteToOmniJsonExpressionLiteral(unscaledValue.child, exprsIndexMap),
            toOmniJsonLiteral(Literal(unscaledValue.child.dataType.asInstanceOf[DecimalType].precision, IntegerType)),
            toOmniJsonLiteral(Literal(unscaledValue.child.dataType.asInstanceOf[DecimalType].scale, IntegetType)))

      // omni not support return null, now rewrite to if(IsOverflowDecimal())? NULL:MakeDecimal()
      case checkOverflow: CheckOverflow =>
        ("{\"exprType\":\"IF\",\"returnType\":%s, \"condition\":{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
          "\"function_name\":\"IsOverflowDecimal\",\"arguments\":[%s,%s,%s,%s,%s]},\"if_true\":%s," +
          "\"if_false\":{\"exprType\":\"FUNCTION\",\"returnType\":%s," +
          "\"function_name\":\"MakeDecimal\", \"arguments\":[%s,%s,%s,%s,%s]}}")
          .format(sparkTypeToOmniExpJsonType(checkOverflow.dataType),
            sparkTypeToOmniExpJsonType(BooleanType),
            rewriteToOmniJsonExpressionLiteral(checkOverflow.child, exprsIndexMap,
              DecimalType(checkOverflow.dataType.precision, checkOverflow.dataType.scale)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.precision, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.scale, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.precision, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.scale, IntegerType)),
            // if_true
            toOmniJsonLiteral(Literal(null, checkOverflow.dataType)),
            // if_false
            sparkTypeToOmniExpJsonType(DecimalType(checkOverflow.dataType.precision, checkOverflow.dataType.scale)),
            rewriteToOmniJsonExpressionLiteral(checkOverflow.child,
              exprsIndexMap,
              DecimalType(checkOverflow.dataType.precision, checkOverflow.dataType.scale))
              toOmniJsonLiteral (Literal(checkOverflow.dataType.precision, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.scale, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.precision, IntegerType)),
            toOmniJsonLiteral(Literal(checkOverflow.dataType.scale, IntegerType)))

      case makeDecimal: MakeDecimal =>
        makeDecimal.child.dataType match {
          case decimalChild: DecimalType =>
            "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"function_name\":\"MakeDecimal\", \"arguments\":[%s,%s,%s,%s,%s]}"
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniJsonExpressionLiteral(makeDecimal.child, exprsIndexMap),
                toOmniJsonLiteral(Literal(decimalChild.precision, IntegerType)),
                toOmniJsonLiteral(Literal(decimalChild.scale, IntegerType)),
                toOmniJsonLiteral(Literal(decimalChild.precision, IntegerType)),
                toOmniJsonLiteral(Literal(decimalChild.scale, IntegerType)))

          case longChild: LongType =>
            "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"function_name\":\"MakeDecimal\", \"arguments\":[%s,%s,%s]}"
              .format(sparkTypeToOmniExpJsonType(makeDecimal.dataType),
                rewriteToOmniJsonExpressionLiteral(makeDecimal.child, exprsIndexMap),
                toOmniJsonLiteral(Literal(decimalChild.precision, IntegerType)),
                toOmniJsonLiteral(Literal(decimalChild.scale, IntegerType)))
          case _ =>
            throw new RuntimeException(s"Unsupported dataType for MakeDecimal: ${makeDecimal.child.dataType}")
        }

      case sub: SubStract =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"SUBSTRACT\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(returnDataType),
            rewriteToOmniJsonExpressionLiteral(sub.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(sub.right, exprsIndexMap))

      case add: Add =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"Add\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(returnDataType),
            rewriteToOmniJsonExpressionLiteral(add.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(add.right, exprsIndexMap))

      case mult: Multiply =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"MULTIPLY\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(returnDataType),
            rewriteToOmniJsonExpressionLiteral(mult.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(mult.right, exprsIndexMap))

      case divide: Divide =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"DIVIDE\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(returnDataType),
            rewriteToOmniJsonExpressionLiteral(divide.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(divide.right, exprsIndexMap))

      case mod: Remainder =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"MODULUS\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(returnDataType),
            rewriteToOmniJsonExpressionLiteral(mod.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(mod.right, exprsIndexMap))

      case greaterThan: GreaterThan =>
        ("{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"GREATER_THAN\", \"left\":%s, \"right\":%s}")
          .format(sparkTypeToOmniExpJsonType(greaterThan.dataType),
            rewriteToOmniJsonExpressionLiteral(greaterThan.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(greaterThan.right, exprsIndexMap))

      case greaterThanOrEq: GreaterThanOrEqual =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"GREATER_THAN_OR_EQUAL\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(greaterThanOrEq.dataType),
            rewriteToOmniJsonExpressionLiteral(greaterThanOrEq.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(greaterThanOrEq.right, exprsIndexMap))

      case lessThan: LessThan =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"LESS_THAN\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(lessThan.dataType),
            rewriteToOmniJsonExpressionLiteral(lessThan.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(lessThan.right, exprsIndexMap))

      case lessThanOrEq: LessThanOrEqual =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"LESS_THAN_OR_EQUAL\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(lessThanOrEq.dataType),
            rewriteToOmniJsonExpressionLiteral(lessThanOrEq.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(lessThanOrEq.right, exprsIndexMap))

      case equal: EqualTo =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"EQUAL\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(equal.dataType),
            rewriteToOmniJsonExpressionLiteral(equal.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(equal.right, exprsIndexMap))

      case or: Or =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"OR\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(or.dataType),
            rewriteToOmniJsonExpressionLiteral(or.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(or.right, exprsIndexMap))

      case and: And =>
        "{\"exprType\":\"BINARY\",\"returnType\":%s, \"operator\":\"AND\", \"left\":%s, \"right\":%s}"
          .format(sparkTypeToOmniExpJsonType(and.dataType),
            rewriteToOmniJsonExpressionLiteral(and.left, exprsIndexMap),
            rewriteToOmniJsonExpressionLiteral(and.right, exprsIndexMap))

      case alias: Alias => rewriteToOmniJsonExpressionLiteral(alias.child, exprsIndexMap)
      case literal: Literal => toOmniJsonLiteral(literal)
      case not: Not =>
        "{\"exprType\":\"UNARY\",\"returnType\":%s, \"operator\":\"not\", \"expr\":%s}".format(
          sparkTypeToOmniExpJsonType(BooleanType),
          rewriteToOmniJsonExpressionLiteral(not.child, exprsIndexMap))

      case isnotnull: IsNotNull =>
        ("{\"exprType\":\"UNARY\",\"returnType\":%s, \"operator\":\"not\"," +
          "\"expr\":{\"exprType\":\"IS_NULL\",\"returnType\":%s, \"arguments\":[%s]}}").format(
          sparkTypeToOmniExpJsonType(BooleanType),
          sparkTypeToOmniExpJsonType(BooleanType),
          rewriteToOmniJsonExpressionLiteral(isnotnull.child, exprsIndexMap))

      case subString: SubString =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"function_name\":\"subString\", \"arguments\":[%s,%s,%s]}".format(
          sparkTypeToOmniExpJsonType(subString.dataType),
          rewriteToOmniJsonExpressionLiteral(subString.str, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(subString.pos, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(subString.len, exprsIndexMap))

      case cast: Cast =>
        unsupportedCastCheck(expr, cast)
        val returnType = sparkTypeToOmniExpJsonType(cast.dataType)
        cast.dataType match {
          case StringType =>
            "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"width\":50,\"function_name\":\"CAST\", \"arguments\":[%s]}".format(
              returnType, rewriteToOmniJsonExpressionLiteral(cast.child, exprsIndexMap))
          case dt: DecimalType =>
            if (cast.child.dataType.isInstanceOf[DoubleType]) {
              "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"CAST\", \"arguments\":[%s,%s,%s]}".format(
                returnType, rewriteToOmniJsonExpressionLiteral(cast.child, exprsIndexMap),
                toOmniJsonLiteral(Literal(dt.precision, IntegerType)),
                toOmniJsonLiteral(Literal(dt.scale, IntegerType)))
            } else {
              rewriteToOmniJsonExpressionLiteral(
                MakeDecimal(cast.child, dt.precision, dt.scale), exprsIndexMap)
            }
          case _ =>
            "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"CAST\", \"arguments\":[%s]}"
              .format(returnType, rewriteToOmniJsonExpressionLiteral(cast.child, exprsIndexMap))
        }

      case abs: Abs =>
        "{\"exprType\":\"FUNCTION\",\"returnType\":%s,\"function_name\":\"abs\", \"arguments\":[%s]}"
          .format(sparkTypeToOmniExpJsonType(abs.dataType),
            rewriteToOmniJsonExpressionLiteral(abs.child, exprsIndexMap))

      case in: In =>
        "{\"exprType\":\"IN\",\"returnType\":%s, \"arguments\":%s}".format(
          sparkTypeToOmniExpJsonType(in.dataType),
          in.children.map(child => rewriteToOmniJsonExpressionLiteral(child, exprsIndexMap))
            .mkString("[", ",", "]"))

      case inset: InSet =>
        "{\"exprType\":\"IN\",\"returnType\":%s, \"arguments\":[%s, %s]}".format(
          sparkTypeToOmniExpJsonType(inset.dataType),
          rewriteToOmniJsonExpressionLiteral(inset.child, exprsIndexMap),
          inset.set.map(child => toOmniJsonLiteral(Literal(child, inset.child.dataType))).mkString("[", ",", "]"))

      case ifExp: If =>
        "{\"exprType\":\"IF\",\"returnType\":%s, \"condition\":%s,\"if_true\":%s,\"if_false\":%s}".format(
          sparkTypeToOmniExpJsonType(ifExp.dataType),
          rewriteToOmniJsonExpressionLiteral(ifExp.predicate, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(ifExp.trueValue, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(ifExp.falseValue, exprsIndexMap))

      case caseWhen: CaseWhen =>
        "{\"exprType\":\"IF\",\"returnType\":%s, \"condition\":%s,\"if_true\":%s,\"if_false\":%s}".format(
          sparkTypeToOmniExpJsonType(caseWhen.dataType),
          rewriteToOmniJsonExpressionLiteral(caseWhen.branches(0)._1, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(caseWhen.branches(0)._2, exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(caseWhen.elseValue.get, exprsIndexMap))

      case coalesce: Coalesce =>
        "{\"exprType\":\"COALESCE\",\"returnType\":%s, \"value1\":%s,\"value2\":%s}".format(
          sparkTypeToOmniExpJsonType(coalesce.dataType),
          rewriteToOmniJsonExpressionLiteral(coalesce.children(0), exprsIndexMap),
          rewriteToOmniJsonExpressionLiteral(coalesce.children(1), exprsIndexMap))

      case concat: Concat =>
        getConcatJsonStr(concat, exprsIndexMap)
      case attr: Attribute => toOmniJsonAttribute(attr, exprsIndexMap(attr.exprId))
      case _ =>
        throw new RuntimeException(s"Unsupported expression: $expr")
    }
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
    val template = "{\"exprType\":\"FUNCTION\",\"returnType\":%s, \"function_name\":\"concat\", \"arguments\":[%s, %s]}"
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
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d, \"width\":%d}"
          .format(omniDataType, colVal, getStringLength(metadata))
      case dt: DecimalType =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d, \"precision\":%s, \"scale\":%s}"
          .format(omniDataType, colVal, dt.precision, dt.scale)
      case _ =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d}".format(omniDataType, colVal)
    }
  }

  def toOmniJsonLiteral(literal: Literal): String = {
    val omniType = sparkTypeToOmniExpType(literal.dataType)
    val value = literal.value
    if (value == null) {
      return "{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b}".format(sparkTypeToOmniExpJsonType(literal.dataType), true)
    }
    literal.dataType match {
      case StringType =>
        ("{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b, \"value\":\"%s\", \"width\":%d}")
          .format(omniType, false, value.toString, value.toString.length)
      case dt: DecimalType =>
        if (DecimalType.is64BitDecimalType(dt)) {
          "{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b, \"value\":%s, \"precision\":%s, \"scale\":%s}"
            .format(omniType, false, value.asInstanceOf[Decimal].toUnscaledLong, dt.precision, dt.scale)
        } else {
          // NOTES: decimal128 literal value need use string format
          "{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b, \"value\":%s, \"precision\":%s, \"scale\":%s}"
            .format(omniType, false, value.asInstanceOf[Decimal].toUnscaledLong, dt.precision, dt.scale)
        }
      case _ =>
        "{\"exprType\":\"LITERAL\",\"dataType\":%s, \"isNull\":%b, \"value\":\"%s\"}".format(omniType, false, value)
    }
  }

  def toOmniAggFunType(agg: AggregateExpression): FunctionType = {
    agg.aggregateFunction match {
      case Sum(_) => OMNI_AGGREGATION_TYPE_SUM
      case Max(_) => OMNI_AGGREGATION_TYPE_MAX
      case Average(_) => OMNI_AGGREGATION_TYPE_AVG
      case Min(_) => OMNI_AGGREGATION_TYPE_MIN
      case RowNumber() => OMNI_AGGREGATION_TYPE_NUMBER
      case Count(Literal(1, Integer) :: Nil) | Count(ArrayBuffer(Literal(1, IntegerType))) =>
        throw new RuntimeException(s"Unsupported count(*) or count(1)")
      case Count() => OMNI_AGGREGATION_TYPE_COUNT_COLUMN
      case _ => throw new RuntimeException(s"Unsupported aggregate function: $agg")
    }
  }

  def toOmniWindowFunType(window: Expression): FunctionType = {
    window match {
      case Rank(_) => OMNI_WINDOW_TYPE_RANK
      case RowNumber() => OMNI_WINDOW_TYPE_ROW_NUMBER
      case _ => throw new RuntimeException(s"Unsupported window function: $window")
    }
  }

  def sparkTypeToOmniExpType(dataType: DataType): String = {
    dataType match {
      case ShortType => OMNI_SHORT_TYPE
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
        throw new RuntimeException(s"Unsupported dataType: $dataType")
    }
  }

  def sparkTypeToOmniExpJsonType(dataType: DataType): String = {
    val omniTypeIdStr = sparkTypeToOmniExpType(dataType)
    dataType match {
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
          new DecimalType64DataType(dt.precision, dt.scale)
        } else {
          new DecimalType128DataType(dt.precision, dt.scale)
        }
      case _ =>
        throw new RuntimeException(s"Unsupported dataType: $dataType")
    }
  }

  def sparkProjectionToOmniJsonProjection(attr: Attribute, colVal: Int): String = {
    val dataType: DataType = attr.dataType
    val metadata = attr.metadata
    val omniDataType: String = sparkTypeToOmniExpType(dataType)
    dataType match {
      case ShortType | IntegerType | LongType | DoubleType | BooleanType | DateType =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d}".format(omniDataType, colVal)
      case StringType =>
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d, \"width\":%d}"
          .format(omniDataType, colVal, getStringLength(metadata))
      case dt: DecimalType =>
        var omniDataType = OMNI_DECIMAL128_TYPE
        if (DecimalType.is64BitDecimalType(dt)) {
          omniDataType = OMNI_DECIMAL64_TYPE
        }
        "{\"exprType\":\"FIELD_REFERENCE\",\"dataType\":%s, \"colVal\":%d, \"precision\":%s, \"scale\":%s}"
          .format(omniDataType, colVal, dt.precision, dt.scale)
      case _ =>
        throw new RuntimeException(s"Unsupported dataType: $dataType")
    }
  }

  private def getStringLength(metadata: Metadata): Int = {
    var width = DEFAULT_STRING_TYPE_LENGTH
    if (getRowTypeString(metadata).isDefined) {
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
}
