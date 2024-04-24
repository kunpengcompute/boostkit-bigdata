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

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.CompareExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ComputeExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        List<ExprNodeDesc> allNode = getAllNode(node);
        boolean hasDecimal128 = false;
        for (ExprNodeDesc item : allNode) {
            if (item.getTypeInfo() instanceof DecimalTypeInfo && ((DecimalTypeInfo) item.getTypeInfo()).getPrecision() > 18) {
                hasDecimal128 = true;
                break;
            }
        }
        conjureNodeType(children);
        CompareExpression compareExpression = new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator);
        for (ExprNodeDesc child : children) {
            BaseExpression baseExpression;
            if (child instanceof ExprNodeGenericFuncDesc) {
                baseExpression = ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector);
            } else {
                baseExpression = ExpressionUtils.createNode(hasDecimal128, child, inspector);
            }
            if (baseExpression.getReturnType() == Decimal64DataType.DECIMAL64.getId().toValue() && hasDecimal128) {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) child.getTypeInfo();
                CastFunctionExpression castFunctionExpression = new CastFunctionExpression(Decimal128DataType.DECIMAL128.getId().toValue(),
                        TypeUtils.getCharWidth(child), decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
                castFunctionExpression.add(baseExpression);
                baseExpression = castFunctionExpression;
            }
            compareExpression.add(baseExpression);
        }
        return compareExpression;
    }

    private List<ExprNodeDesc> getAllNode(ExprNodeGenericFuncDesc node) {
        List<ExprNodeDesc> children = node.getChildren();
        if (children.isEmpty()) {
            return Collections.singletonList(node);
        }
        List<ExprNodeDesc> allNode = new ArrayList<>();
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                allNode.addAll(getAllNode((ExprNodeGenericFuncDesc) child));
            }
            allNode.add(child);
        }
        return allNode;
    }

    private void conjureNodeType(List<ExprNodeDesc> nodes) {
        ExprNodeDesc markNode = null;
        TypeInfo replaceTypeInfo = null;
        for (ExprNodeDesc node : nodes) {
            if (node instanceof ExprNodeConstantDesc) {
                markNode = node;
            } else {
                replaceTypeInfo = node.getTypeInfo();
            }
        }
        if (markNode != null && replaceTypeInfo != null) {
            markNode.setTypeInfo(replaceTypeInfo);
        }
    }
}
