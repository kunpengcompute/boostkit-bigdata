/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.DecimalReference;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.type.DataType;
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JoinUtils {
    public static List<ExprNodeEvaluator> getExprNodeColumnEvaluator(List<ExprNodeEvaluator> joinKeys) {
        return getExprNodeColumnEvaluator(joinKeys, false);
    }

    public static List<ExprNodeEvaluator> getExprNodeColumnEvaluator(List<ExprNodeEvaluator> joinKeys, boolean includeConstant) {
        List<ExprNodeEvaluator> exprNodeColumnEvaluators = new ArrayList<>();
        for (ExprNodeEvaluator joinKey : joinKeys) {
            if (includeConstant && joinKey instanceof ExprNodeConstantEvaluator) {
                exprNodeColumnEvaluators.add(joinKey);
            }
            dealChildren(joinKey, exprNodeColumnEvaluators);
        }
        return exprNodeColumnEvaluators;
    }

    private static void dealChildren(ExprNodeEvaluator joinKey, List<ExprNodeEvaluator> exprNodeColumnEvaluators) {
        if (joinKey instanceof ExprNodeColumnEvaluator) {
            exprNodeColumnEvaluators.add(joinKey);
            return;
        }
        if (joinKey.getChildren() == null) {
            return;
        }
        Arrays.stream(joinKey.getChildren()).forEach(child -> dealChildren(child, exprNodeColumnEvaluators));
    }

    public static String[] getExprFromExprNode(List<ExprNodeEvaluator> nodes, Map<String, Integer> keyColNameToId, ObjectInspector inspector) {
        List<String> expressions = new ArrayList<>();
        for (ExprNodeEvaluator node : nodes) {
            if (node instanceof ExprNodeGenericFuncEvaluator) {
                expressions.add(ExpressionUtils.buildSimplify((ExprNodeGenericFuncDesc) node.getExpr(), inspector).toString());
                continue;
            } else if (node instanceof ExprNodeConstantEvaluator) {
                expressions.add(ExpressionUtils.createLiteralNode(node.getExpr()).toString());
                continue;
            }
            PrimitiveTypeInfo keyType = (PrimitiveTypeInfo) node.getExpr().getTypeInfo();
            PrimitiveTypeInfo inputType = ((AbstractPrimitiveObjectInspector) node.getOutputOI()).getTypeInfo();
            if (!keyType.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) || keyType.equals(inputType)) {
                expressions.add(TypeUtils.buildExpression(((AbstractPrimitiveObjectInspector) node.getOutputOI()).getTypeInfo(),
                        keyColNameToId.get(((ExprNodeColumnEvaluator) node).getExpr().getColumn())));
                continue;
            }
            int returnType = TypeUtils.convertHiveTypeToOmniType(keyType);
            CastFunctionExpression cast = new CastFunctionExpression(returnType, TypeUtils.getCharWidth(node.getExpr()),
                    ((DecimalTypeInfo) keyType).getPrecision(), ((DecimalTypeInfo) keyType).getScale());
            int fieldID = ((StructObjectInspector) inspector).getStructFieldRef(node.getExpr().getExprString()).getFieldID();
            int omniType = TypeUtils.convertHiveTypeToOmniType(inputType);
            BaseExpression decimalReference = new DecimalReference(fieldID, omniType, ((DecimalTypeInfo) inputType).getPrecision(),
                    ((DecimalTypeInfo) inputType).getScale());
            cast.add(decimalReference);
            expressions.add(cast.toString());
        }
        return expressions.toArray(new String[0]);
    }

    public static DataType[] getTypeFromInspectors(List<ObjectInspector> inspectors) {
        return inspectors.stream().map(
                        inspector -> TypeUtils.buildInputDataType(((AbstractPrimitiveObjectInspector) inspector).getTypeInfo()))
                .toArray(DataType[]::new);
    }
}
