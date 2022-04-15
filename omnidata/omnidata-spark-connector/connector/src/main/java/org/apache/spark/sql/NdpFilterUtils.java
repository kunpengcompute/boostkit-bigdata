package org.apache.spark.sql;

import static io.prestosql.spi.relation.SpecialForm.Form.IN;
import static io.prestosql.spi.relation.SpecialForm.Form.IS_NULL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * NdpFilterUtils
 */
public class NdpFilterUtils {

    public static int getFilterProjectionId(Expression expression, Map<String, Integer> fieldMap) {
        String filterColumnName = expression.toString().split("#")[0];
        if (fieldMap.containsKey(filterColumnName)) {
            return fieldMap.get(filterColumnName);
        } else {
            return fieldMap.size();
        }
    }

    public static  RowExpression generateRowExpression(
        String signatureName, PrestoExpressionInfo expressionInfo,
        Type prestoType, int filterProjectionId,
        List<Object> argumentValues,
        List<RowExpression> multiArguments, String operatorName) {
        RowExpression rowExpression;
        List<RowExpression> rowArguments;
        Signature signature = new Signature(
            QualifiedObjectName.valueOfDefaultFunction("$operator$" +
            signatureName.toLowerCase(Locale.ENGLISH)),
            FunctionKind.SCALAR, new TypeSignature("boolean"),
            new TypeSignature(prestoType.toString()), new TypeSignature(prestoType.toString()));
        switch (operatorName.toLowerCase(Locale.ENGLISH)) {
            case "is_null":
                List<RowExpression> notnullArguments = new ArrayList<>();
                notnullArguments.add(new InputReferenceExpression(filterProjectionId, prestoType));
                rowExpression = new SpecialForm(IS_NULL, BOOLEAN, notnullArguments);
                break;
            case "in":
                rowArguments = getConstantArguments(prestoType, argumentValues, filterProjectionId);
                rowExpression = new SpecialForm(IN, BOOLEAN, rowArguments);
                break;
            case "multy_columns":
                Signature signatureMulti = new Signature(
                    QualifiedObjectName.valueOfDefaultFunction("$operator$"
                    + signatureName.toLowerCase(Locale.ENGLISH)),
                    FunctionKind.SCALAR, new TypeSignature("boolean"),
                    new TypeSignature(prestoType.toString()),
                    new TypeSignature(prestoType.toString()));
                rowExpression = new CallExpression(signatureName,
                    new BuiltInFunctionHandle(signatureMulti), BOOLEAN, multiArguments);
                break;
            case "isempty":
            case "isdeviceidlegal":
            case "ismessycode":
                rowExpression = expressionInfo.getPrestoRowExpression();
                break;
            default:
                if (expressionInfo.getReturnType() != null) {
                    rowArguments = getUdfArguments(prestoType,
                        argumentValues, expressionInfo.getPrestoRowExpression());
                } else {
                    rowArguments = getConstantArguments(prestoType,
                        argumentValues, filterProjectionId);
                }
                rowExpression = new CallExpression(signatureName,
                    new BuiltInFunctionHandle(signature), BOOLEAN, rowArguments);
                break;
        }
        return rowExpression;
    }

    public static List<RowExpression> getConstantArguments(Type typeStr,
        List<Object> argumentValues,
        int columnId) {
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(new InputReferenceExpression(columnId, typeStr));
        if (null != argumentValues && argumentValues.size() > 0) {
            for (Object argumentValue : argumentValues) {
                arguments.add(NdpUtils
                    .transArgumentData(argumentValue.toString(), typeStr));
            }
        }
        return arguments;
    }

    public static  List<RowExpression> getUdfArguments(Type typeStr, List<Object> argumentValues,
        RowExpression callExpression) {
        List<RowExpression> arguments = new ArrayList<>();
        arguments.add(callExpression);
        if (null != argumentValues && argumentValues.size() > 0) {
            for (Object argumentValue : argumentValues) {
                arguments.add(NdpUtils
                    .transArgumentData(argumentValue.toString(), typeStr));
            }
        }
        return arguments;
    }
}
