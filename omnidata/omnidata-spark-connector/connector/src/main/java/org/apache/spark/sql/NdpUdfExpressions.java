package org.apache.spark.sql;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import scala.collection.JavaConverters;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayItem;
import org.apache.spark.sql.catalyst.expressions.Length;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Lower;
import org.apache.spark.sql.catalyst.expressions.StringInstr;
import org.apache.spark.sql.catalyst.expressions.StringReplace;
import org.apache.spark.sql.catalyst.expressions.StringSplit;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.sql.catalyst.expressions.Upper;
import org.apache.spark.sql.hive.HiveSimpleUDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Used to process Spark`s UDF, which is converted to presto.
 *
 * @since 2021-06-24
 */
public class NdpUdfExpressions {

    private void checkAttributeReference(Expression childExpression,
        PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap, Type childType, List<RowExpression> rowArguments) {
        if ((childExpression instanceof AttributeReference)) {
            int lengthProjectId = NdpFilterUtils.getFilterProjectionId(childExpression, fieldMap);
            rowArguments.add(new InputReferenceExpression(lengthProjectId, childType));
            prestoExpressionInfo.setProjectionId(lengthProjectId);
            prestoExpressionInfo.setFieldDataType(
                NdpUtils.transOlkDataType(childExpression.dataType(), false));
            prestoExpressionInfo.setChildExpression(childExpression);
        } else {
            createNdpUdf(childExpression, prestoExpressionInfo, fieldMap);
            rowArguments.add(prestoExpressionInfo.getPrestoRowExpression());
        }
    }

    /**
     * create Udf
     */
    public void createNdpUdf(Expression udfExpression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        if (udfExpression instanceof Length) {
            createNdpLength((Length) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Upper) {
            createNdpUpper((Upper) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Lower) {
            createNdpLower((Lower) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Cast) {
            createNdpCast((Cast) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Substring) {
            createNdpSubstring((Substring) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringReplace) {
            createNdpReplace((StringReplace) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringInstr) {
            createNdpInstr((StringInstr) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringSplit) {
            createNdpSplit((StringSplit) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof GetArrayItem) {
            createNdpSubscript((GetArrayItem) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof HiveSimpleUDF) {
            createHiveSimpleUdf(udfExpression, prestoExpressionInfo, fieldMap);
        } else {
            throw new RuntimeException("unsupported this UDF:" + udfExpression.toString());
        }
    }

    /**
     * Used to create UDF with only a single parameter
     */
    private void createNdpSingleParameter(NdpUdfEnum udfEnum,
        Expression expression, Expression childExpression,
        PrestoExpressionInfo prestoExpressionInfo, Map<String, Integer> fieldMap) {
        String signatureName = udfEnum.getSignatureName();
        Type childType = NdpUtils.transOlkDataType(childExpression.dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(childExpression,
            prestoExpressionInfo, fieldMap, childType, rowArguments);
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(udfEnum.getOperatorName()),
            FunctionKind.SCALAR, new TypeSignature(
                returnType.toString()), new TypeSignature(childType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpLength(Length expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.LENGTH,
            expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpUpper(Upper expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.UPPER,
            expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpLower(Lower expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.LOWER,
            expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpCast(Cast expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.CAST,
            expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createHiveSimpleUdf(Expression hiveSimpleUDFExpression,
        PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = ((HiveSimpleUDF) hiveSimpleUDFExpression).name();
        List<Expression> hiveSimpleUdf = JavaConverters.seqAsJavaList(
            hiveSimpleUDFExpression.children());
        Type returnType = NdpUtils.transOlkDataType(
            hiveSimpleUDFExpression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        Type strTypeCandidate = returnType;
        for (Expression hiveUdf : hiveSimpleUdf) {
            strTypeCandidate = NdpUtils.transOlkDataType(hiveUdf.dataType(), true);
            checkAttributeReference(hiveUdf, prestoExpressionInfo,
                fieldMap, strTypeCandidate, rowArguments);
        }
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(signatureName),
            FunctionKind.SCALAR, new TypeSignature(returnType.toString()),
            new TypeSignature(strTypeCandidate.toString()));
        RowExpression resExpression = new CallExpression(signatureName.toLowerCase(Locale.ENGLISH),
            new BuiltInFunctionHandle(signature), returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSubstring(Substring expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SUBSTRING.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), true);
        Type lenType = NdpUtils.transOlkDataType(expression.len().dataType(), true);
        Type posType = NdpUtils.transOlkDataType(expression.pos().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
            prestoExpressionInfo, fieldMap, strType, rowArguments);
        rowArguments.add(NdpUtils.transArgumentData(
            expression.pos().toString(), posType));
        rowArguments.add(NdpUtils.transArgumentData(
            expression.len().toString(), lenType));
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(
                NdpUdfEnum.SUBSTRING.getOperatorName()), FunctionKind.SCALAR,
            new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
            new TypeSignature(posType.toString()), new TypeSignature(lenType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
        prestoExpressionInfo.setReturnType(returnType);
    }

    private void createNdpReplace(StringReplace expression,
        PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.REPLACE.getSignatureName();
        Type srcType = NdpUtils.transOlkDataType(expression.srcExpr().dataType(), true);
        Type searchType = NdpUtils.transOlkDataType(
            expression.searchExpr().dataType(), true);
        Type replaceType = NdpUtils.transOlkDataType(
            expression.replaceExpr().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.srcExpr(),
            prestoExpressionInfo, fieldMap, srcType, rowArguments);
        rowArguments.add(NdpUtils.transArgumentData(
            expression.searchExpr().toString(), searchType));
        rowArguments.add(NdpUtils.transArgumentData(
            expression.replaceExpr().toString(), replaceType));
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(
                NdpUdfEnum.REPLACE.getOperatorName()), FunctionKind.SCALAR,
            new TypeSignature(returnType.toString()), new TypeSignature(srcType.toString()),
            new TypeSignature(searchType.toString()), new TypeSignature(replaceType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpInstr(StringInstr expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.INSTR.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), true);
        Type substrType = NdpUtils.transOlkDataType(expression.substr().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
            prestoExpressionInfo, fieldMap, strType, rowArguments);
        rowArguments.add(NdpUtils.transArgumentData(
            expression.substr().toString(), substrType));
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(
                NdpUdfEnum.INSTR.getOperatorName()), FunctionKind.SCALAR,
            new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
            new TypeSignature(substrType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSplit(StringSplit expression, PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SPLIT.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), true);
        Type regexType = NdpUtils.transOlkDataType(expression.regex().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
            prestoExpressionInfo, fieldMap, strType, rowArguments);
        rowArguments.add(NdpUtils.transArgumentData(
            expression.regex().toString(), regexType));
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(
                NdpUdfEnum.SPLIT.getOperatorName()), FunctionKind.SCALAR,
            new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
            new TypeSignature(regexType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSubscript(GetArrayItem expression,
        PrestoExpressionInfo prestoExpressionInfo,
        Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SUBSCRIPT.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.child().dataType(), true);
        Type ordinalType = NdpUtils.transOlkDataType(
            expression.ordinal().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.child(),
            prestoExpressionInfo, fieldMap, strType, rowArguments);
        // The presto`s array subscript is initially 1.
        int argumentValue = Integer.parseInt(
            ((Literal) expression.ordinal()).value().toString()) + 1;
        rowArguments.add(NdpUtils.transArgumentData(
            Integer.toString(argumentValue), ordinalType));
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction(
                NdpUdfEnum.SUBSCRIPT.getOperatorName()), FunctionKind.SCALAR,
            new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
            new TypeSignature(ordinalType.toString()));
        RowExpression resExpression = new CallExpression(
            signatureName, new BuiltInFunctionHandle(signature),
            returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }
}
