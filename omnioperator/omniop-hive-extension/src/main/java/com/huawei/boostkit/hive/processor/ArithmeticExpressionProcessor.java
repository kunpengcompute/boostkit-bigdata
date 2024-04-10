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

package com.huawei.boostkit.hive.processor;

import static com.huawei.boostkit.hive.expression.TypeUtils.getMaxScale;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.DivideExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.util.HashMap;
import java.util.List;

public class ArithmeticExpressionProcessor implements ExpressionProcessor {
    private final HashMap<Class<? extends GenericUDF>, String> OPERATOR = new HashMap<Class<? extends GenericUDF>, String>() {
        {
            put(GenericUDFOPDivide.class, "DIVIDE");
            put(GenericUDFOPMultiply.class, "MULTIPLY");
            put(GenericUDFOPMod.class, "MODULUS");
            put(GenericUDFOPPlus.class, "ADD");
            put(GenericUDFOPMinus.class, "SUBTRACT");
        }
    };

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        Integer precision = null;
        Integer scale = null;
        String functionName = node.getGenericUDF().getClass().getSimpleName();
        boolean needUpscale = functionName.equals("GenericUDFOPMultiply") || functionName.equals("GenericUDFOPDivide")
                || functionName.equals("GenericUDFOPMod");
        int maxScale = 0;
        if (node.getTypeInfo() instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) node.getTypeInfo()).getPrecision();
            scale = ((DecimalTypeInfo) node.getTypeInfo()).getScale();
            maxScale = getMaxScale(children, scale);
        }
        int returnType = TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo());
        DivideExpression compareExpression = new DivideExpression(returnType,
                OPERATOR.get(node.getGenericUDF().getClass()), precision, scale);
        for (ExprNodeDesc child : children) {
            BaseExpression childNode;
            if (child instanceof ExprNodeGenericFuncDesc) {
                childNode = ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector);
            } else {
                childNode = ExpressionUtils.createNode(child, inspector);
            }
            if (!child.getTypeInfo().equals(node.getTypeInfo())) {
                addCast(node, precision, scale, needUpscale, maxScale, returnType, compareExpression, child, childNode);
            } else {
                compareExpression.add(childNode);
            }
        }
        return compareExpression;
    }

    private void addCast(ExprNodeGenericFuncDesc node, Integer precision, Integer scale, boolean needUpscale, int maxScale, int returnType, DivideExpression compareExpression, ExprNodeDesc child, BaseExpression childNode) {
        Integer childPrecision = null;
        Integer childScale = null;
        if (child.getTypeInfo() instanceof DecimalTypeInfo && needUpscale) {

            childScale = ((DecimalTypeInfo) child.getTypeInfo()).getScale();
            childPrecision = ((DecimalTypeInfo) child.getTypeInfo()).getPrecision();
            if (maxScale == childScale) {
                compareExpression.add(childNode);
                return;
            } else {
                childPrecision = Math.min(Math.max(childPrecision + maxScale - childScale, precision), 38);
                childScale = maxScale;
            }
        } else {
            childPrecision = precision;
            childScale = scale;
        }
        CastFunctionExpression functionExpression = new CastFunctionExpression(returnType,
                TypeUtils.getCharWidth(node), childPrecision, childScale);
        compareExpression.add(ExpressionUtils.optimizeCast(childNode, functionExpression));
    }
}
