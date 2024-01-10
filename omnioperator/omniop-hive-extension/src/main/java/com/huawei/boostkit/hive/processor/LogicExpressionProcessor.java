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
import com.huawei.boostkit.hive.expression.CompareExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.LinkedList;

public class LogicExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        BaseExpression root = new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator);
        LinkedList<ExprNodeDesc> list = new LinkedList<>(node.getChildren());
        int level = 1;
        int count = 0;
        int originSize = list.size();
        while (!list.isEmpty()) {
            ExprNodeDesc next = list.poll();
            BaseExpression leaf;
            if (next instanceof ExprNodeGenericFuncDesc) {
                leaf = createFunctionNode((ExprNodeGenericFuncDesc) next, inspector);
            } else if (next instanceof ExprNodeColumnDesc) {
                leaf = ExpressionUtils.createReferenceNode(next, inspector);
            } else if (next instanceof ExprNodeConstantDesc) {
                leaf = ExpressionUtils.createLiteralNode(next);
            } else {
                continue;
            }
            leaf.setLevel(level);
            root.add(leaf);
            if (next.getChildren() != null && !leaf.isFull()) {
                list.addAll(next.getChildren());
            }
            if (++count == originSize) {
                level++;
                originSize = list.size();
                count = 0;
            }
        }

        return root;
    }

    private BaseExpression createFunctionNode(ExprNodeGenericFuncDesc next, ObjectInspector inspector) {
        BaseExpression leaf;
        ExpressionProcessor expressionProcessor = ExpressionUtils.UDF_TO_PROCESSOR.get(
                next.getGenericUDF().getClass());
        if (expressionProcessor != null) {
            leaf = expressionProcessor.process(next, TypeUtils.getOperatorDesc(
                    next.getGenericUDF()), inspector);
        } else {
            leaf = new CompareExpression("BINARY",
                    TypeUtils.convertHiveTypeToOmniType(next.getTypeInfo()),
                    TypeUtils.getOperatorDesc(next.getGenericUDF()));
        }
        return leaf;
    }
}
