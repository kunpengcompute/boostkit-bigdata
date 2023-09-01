/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.olk.operator.filterandproject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.UnknownType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.WhenClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static nova.hetu.olk.tool.OperatorUtils.toDataType;

/**
 * Util class for openLooKeng RowExpression
 *
 * @since 20210901
 */
public class OmniRowExpressionUtil
{
    /**
     * Enum type to choose which format to parse RowExpression into
     */
    public enum Format
    {
        STRING,
        JSON;
    }

    private OmniRowExpressionUtil()
    {
    }

    // Unchecked Exception wrapper
    private static class RowExpressionJsonProcessingException
            extends RuntimeException
    {
        RowExpressionJsonProcessingException(Throwable e)
        {
            super(e);
        }
    }

    /**
     * Wrapper function that selectively stringify RowExpression into string /
     * jsonString
     *
     * @param rowExpression RowExpression from openLooKeng
     * @param format enum var indicates returning String / JSON String
     * @return String / JSON String representation of the RowExpression
     */
    public static String expressionStringify(RowExpression rowExpression, Format format)
    {
        switch (format) {
            case JSON:
                String expressionJsonString = null;
                try {
                    expressionJsonString = expressionJsonify(rowExpression);
                }
                catch (JsonProcessingException e) {
                    throw new RowExpressionJsonProcessingException(e);
                }
                return expressionJsonString;
            case STRING:
            default:
                return expressionStringify(rowExpression);
        }
    }

    /**
     * Stringify a RowExpression
     *
     * @param rowExpression RowExpression from openLooKeng
     * @return String representation of the RowExpression
     */
    public static String expressionStringify(RowExpression rowExpression)
    {
        if (rowExpression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) rowExpression;
            List<String> args = callExpression.getArguments().stream().map(OmniRowExpressionUtil::expressionStringify)
                    .collect(Collectors.toList());
            return callExpression.getDisplayName() + ":" + typeToDataTypeId(callExpression.getType()) + "("
                    + Joiner.on(", ").join(args) + ")";
        }

        if (rowExpression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) rowExpression;
            List<String> args = specialForm.getArguments().stream().map(OmniRowExpressionUtil::expressionStringify)
                    .collect(Collectors.toList());
            return specialForm.getForm().name() + ":" + toDataType(specialForm.getType()).getId().ordinal() + "("
                    + Joiner.on(", ").join(args) + ")";
        }

        if (rowExpression instanceof LambdaDefinitionExpression) {
            LambdaDefinitionExpression lambdaDefinitionExpression = (LambdaDefinitionExpression) rowExpression;
            return "(" + Joiner.on(", ").join(lambdaDefinitionExpression.getArguments()) + ") -> "
                    + lambdaDefinitionExpression.getBody();
        }

        if (rowExpression instanceof InputReferenceExpression || rowExpression instanceof VariableReferenceExpression) {
            return rowExpression.toString();
        }

        if (rowExpression instanceof ConstantExpression) {
            ConstantExpression constantExpression = (ConstantExpression) rowExpression;
            Type type = rowExpression.getType();
            if (type instanceof UnknownType && constantExpression.getValue() == null) {
                return "NULL:0";
            }

            if ((type instanceof VarcharType || type instanceof CharType)
                    && constantExpression.getValue() instanceof Slice) {
                String varcharValue = ((Slice) constantExpression.getValue()).toStringAscii();
                return "'" + varcharValue + "':" + typeToDataTypeId(type);
            }

            if (type instanceof DecimalType && !((DecimalType) type).isShort()
                    && constantExpression.getValue() instanceof Slice) {
                return Decimals.decodeUnscaledValue((Slice) constantExpression.getValue()) + ":"
                        + typeToDataTypeId(type);
            }

            return constantExpression.getValue() + ":" + typeToDataTypeId(type);
        }
        return rowExpression.toString();
    }

    /**
     * Get DataTypeId with width from the type signature
     *
     * @param type type from openLooKeng
     * @return DataTypeId corresponding to Type
     */
    public static String typeToDataTypeId(Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        if ("char".equalsIgnoreCase(signature.getBase())) {
            int width = signature.getParameters().get(0).getLongLiteral().intValue();
            return String.format("%d[%d]", toDataType(type).getId().ordinal(), width);
        }
        return String.valueOf(toDataType(type).getId().ordinal());
    }

    /**
     * Jsonify a RowExpression into JSON string
     *
     * @param rowExpression RowExpression from openLooKeng
     * @return String representation of the RowExpression in JSON structure
     * @throws JsonProcessingException when error converting JSON to String
     */
    public static String expressionJsonify(RowExpression rowExpression) throws JsonProcessingException
    {
        ObjectNode jsonRoot = rowExpression.accept(new JsonifyVisitor(), null);
        return new ObjectMapper().writeValueAsString(jsonRoot);
    }

    /**
     * Generates a RowExpression to ensure like query compatibility with omniruntime
     *
     * @param staticExpr Expression from openLookeng
     * @param translatedExpr RowExpression from staticFilter conversion
     * @return new Optional<RowExpression> with new, regex-syntax arguments for like
     * queries
     */
    public static Optional<RowExpression> generateOmniExpr(Expression staticExpr, RowExpression translatedExpr)
    {
        // if expression is a when-then expr, we use a separate help function to help
        // parse the expr
        if (staticExpr instanceof SearchedCaseExpression && translatedExpr instanceof SpecialForm) {
            return getWhenExpr((SearchedCaseExpression) staticExpr, (SpecialForm) translatedExpr);
        }

        if (translatedExpr instanceof SpecialForm) {
            SpecialForm specialExpr = (SpecialForm) translatedExpr;
            List<RowExpression> newArguments = new ArrayList<RowExpression>();
            for (int i = 0; i < specialExpr.getArguments().size(); i++) {
                RowExpression nestedExpr = specialExpr.getArguments().get(i);
                if (nestedExpr instanceof SpecialForm || nestedExpr instanceof CallExpression) {
                    newArguments.add(generateOmniExpr((Expression) staticExpr.getChildren().get(i), nestedExpr).get());
                }
                else {
                    newArguments.add(specialExpr.getArguments().get(i));
                }
            }
            Optional<RowExpression> newOmniFilter = Optional
                    .of(new SpecialForm(specialExpr.getForm(), specialExpr.getType(), newArguments));
            return newOmniFilter;
        }
        else if (translatedExpr instanceof CallExpression) {
            CallExpression callExpr = (CallExpression) translatedExpr;
            List<RowExpression> newArguments = new ArrayList<RowExpression>();
            for (int i = 0; i < callExpr.getArguments().size(); i++) {
                RowExpression nestedExpr = callExpr.getArguments().get(i);
                if (nestedExpr instanceof SpecialForm || nestedExpr instanceof CallExpression) {
                    newArguments.add(generateOmniExpr((Expression) staticExpr.getChildren().get(i), nestedExpr).get());
                }
                else {
                    newArguments.add(callExpr.getArguments().get(i));
                }
            }
            Optional<RowExpression> newOmniFilter = Optional.of(new CallExpression(callExpr.getDisplayName(),
                    callExpr.getFunctionHandle(), callExpr.getType(), newArguments));

            if ("LIKE".equals(((CallExpression) newOmniFilter.get()).getDisplayName().toUpperCase(Locale.ROOT))) {
                String sqlString = "";
                if (staticExpr.getChildren().get(1) instanceof Cast) {
                    sqlString = ((StringLiteral) ((Cast) staticExpr.getChildren().get(1)).getExpression()).getValue();
                }
                else {
                    sqlString = ((StringLiteral) staticExpr.getChildren().get(1)).getValue();
                }
                return generateLikeExpr(sqlString, newOmniFilter);
            }
            return newOmniFilter;
        }
        return Optional.of(translatedExpr);
    }

    /**
     * Generates a RowExpression for when-then expression special case
     *
     * @param staticExpr when-then expression
     * @param translatedExpr RowExpression from staticFilter conversion
     * @return new Optional<RowExpression> checked for possible like expression
     * nested within
     * note: refer to
     * SqlToRowExpressionTranslator.visitSearchedCaseExpression to see how
     * when-then expressions
     * are being translated. This method involves some assumptions so some
     * comments have been provided
     */
    private static Optional<RowExpression> getWhenExpr(SearchedCaseExpression staticExpr, SpecialForm translatedExpr)
    {
        // generates a list of the expr involved in the nested when-then expression and
        // sends it to helper
        List<Expression> exprList = new ArrayList<Expression>();
        for (WhenClause clause : staticExpr.getWhenClauses()) {
            exprList.add(clause.getOperand());
            exprList.add(clause.getResult());
        }
        boolean defaultValueExist = (((SearchedCaseExpression) staticExpr).getDefaultValue()).isPresent();
        exprList.add(defaultValueExist ? (((SearchedCaseExpression) staticExpr).getDefaultValue()).get() : null);

        List<RowExpression> newArguments = new ArrayList<RowExpression>();
        LinkedList<Type> typeList = new LinkedList<Type>();
        SpecialForm originalExpr = translatedExpr;
        int i = 0;
        // looping through translated expression in the same order as the exprList and
        // matching each child with
        // one another, and running the generateOmniExpr method to check for like
        // expressions
        while (true) {
            typeList.addFirst(translatedExpr.getType());
            // if we reached the final 3 elements within the exprList, we know we have
            // gotten to the last, base layer
            // of SpecialForm Expr in the translatedExpr. Here, we look to match all 3
            // argument of the expr rather
            // than matching 2 and recursivly calling the next layer, as this is the last
            // layer.
            if (i >= exprList.size() - 3) {
                for (int j = 0; j < translatedExpr.getArguments().size(); i++, j++) {
                    newArguments.add((generateOmniExpr(exprList.get(i), translatedExpr.getArguments().get(j))).get());
                }
                break;
            }
            else {
                for (int j = 0; j < translatedExpr.getArguments().size() - 1; i++, j++) {
                    newArguments.add((generateOmniExpr(exprList.get(i), translatedExpr.getArguments().get(j))).get());
                }
                translatedExpr = (SpecialForm) translatedExpr.getArguments().get(2);
            }
        }
        // Recreating the SpecialFrom RowExpression in the same recursive format has it
        // was done in
        // SqlToRowExpressionTranslator.visitSearchedCaseExpression
        Collections.reverse(newArguments);
        RowExpression expression = new SpecialForm(originalExpr.getForm(), typeList.removeFirst(), newArguments.get(2),
                newArguments.get(1), newArguments.get(0));
        for (int j = 3; j < newArguments.size(); j += 2) {
            expression = new SpecialForm(originalExpr.getForm(), typeList.removeFirst(), newArguments.get(j + 1),
                    newArguments.get(j), expression);
        }
        return Optional.of(expression);
    }

    /**
     * Converts SQL like query syntax into regex syntax and recreate return new
     * appropriate filter
     *
     * @param rawString String from OmniLocalExecutionPlanner
     * @param translatedExpr Optional<RowExpression> from
     * OmniLocalExecutionPlanner
     * @return new Optional<RowExpression> with new regex syntax argument
     */
    public static Optional<RowExpression> generateLikeExpr(String rawString, Optional<RowExpression> translatedExpr)
    {
        List<RowExpression> newArgs = new LinkedList<RowExpression>();
        StringBuilder regexString = new StringBuilder(rawString.length() * 2 + 2);
        regexString.append('^');
        for (char curChar : rawString.toCharArray()) {
            switch (curChar) {
                case '%':
                    regexString.append(".*");
                    break;
                case '_':
                    regexString.append(".");
                    break;
                case '\\':
                case '^':
                case '$':
                case '.':
                case '*':
                    regexString.append("\\");
                    regexString.append(curChar);
                    break;
                default:
                    regexString.append(curChar);
                    break;
            }
        }
        regexString.append('$');

        ConstantExpression regexStringSlice = new ConstantExpression(Slices.utf8Slice(regexString.toString()), VARCHAR);
        newArgs.add(((CallExpression) translatedExpr.get()).getArguments().get(0));
        newArgs.add(regexStringSlice);
        Optional<RowExpression> likeTranslatedFilter = Optional
                .of(new CallExpression(((CallExpression) translatedExpr.get()).getDisplayName().toUpperCase(Locale.ROOT),
                        ((CallExpression) translatedExpr.get()).getFunctionHandle(),
                        ((CallExpression) translatedExpr.get()).getType(), newArgs));
        return likeTranslatedFilter;
    }
}
