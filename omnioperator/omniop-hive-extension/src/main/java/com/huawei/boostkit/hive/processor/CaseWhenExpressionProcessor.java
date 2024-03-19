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
import com.huawei.boostkit.hive.expression.ConditionExpression;
import com.huawei.boostkit.hive.expression.DecimalLiteral;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.LiteralFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class CaseWhenExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        Queue<ExprNodeDesc> queue = new LinkedBlockingDeque<>(children);
        TypeInfo typeInfo = node.getTypeInfo();
        Integer precision = null;
        Integer scale = null;
        if (typeInfo instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) typeInfo).getPrecision();
            scale = ((DecimalTypeInfo) typeInfo).getScale();
        }

        ConditionExpression conditionExpression = new ConditionExpression(
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                TypeUtils.calculateVarcharLength(node),
                precision,
                scale);

        while (!queue.isEmpty()) {
            ExprNodeDesc poll = queue.poll();
            conditionExpression.add(createConditionExpression(node, poll, inspector));
            poll = queue.poll();
            conditionExpression.add(createConditionExpression(node, poll, inspector));
            if (queue.size() > 1) {
                ConditionExpression temp = conditionExpression.copyBase();
                conditionExpression.add(temp);
            } else if (queue.size() == 1) {
                conditionExpression.add(createConditionExpression(node, queue.poll(), inspector));
            } else {
                LiteralFactor<Void> literalFactor = new LiteralFactor<>("LITERAL",
                        null, null, null,
                        TypeUtils.getCharWidth(node), TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()));
                conditionExpression.add(literalFactor);
            }
        }
        return conditionExpression;
    }

    private BaseExpression createConditionExpression(ExprNodeGenericFuncDesc root, ExprNodeDesc nodeDesc, ObjectInspector inspector) {
        if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            return ExpressionUtils.build((ExprNodeGenericFuncDesc) nodeDesc, inspector);
        } else if (nodeDesc instanceof ExprNodeConstantDesc) {
            if (nodeDesc.getTypeInfo().getTypeName().equals("void")) {
                if (root.getTypeInfo() instanceof DecimalTypeInfo) {
                    return new DecimalLiteral(null, TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()),
                            ((DecimalTypeInfo) root.getTypeInfo()).getPrecision(),
                            ((DecimalTypeInfo) root.getTypeInfo()).getScale());
                }
                return new LiteralFactor<Void>("LITERAL",
                        null, null, null, TypeUtils.calculateVarcharLength(root),
                        TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()));
            }
            if (root.getTypeInfo() instanceof DecimalTypeInfo && ((ExprNodeConstantDesc) nodeDesc).getValue().equals(0)) {
                return new DecimalLiteral(((DecimalTypeInfo) root.getTypeInfo()).getPrecision() > 18 ? "0" : 0L, TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()),
                        ((DecimalTypeInfo) root.getTypeInfo()).getPrecision(),
                        ((DecimalTypeInfo) root.getTypeInfo()).getScale());
            }
            return ExpressionUtils.createNode(nodeDesc, inspector);
        } else {
            return ExpressionUtils.createNode(nodeDesc, inspector);
        }
    }
}
