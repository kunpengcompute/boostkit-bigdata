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
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.FunctionExpression;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class ConcatExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        int len = TypeUtils.calculateVarcharLength(node);
        if (children.size() == 2) {
            FunctionExpression concatNode = new FunctionExpression(
                    TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                    "concat", 2, len);
            ExprNodeDesc exprNodeDesc = children.get(0);
            concatNode.add(ExpressionUtils.createNode(exprNodeDesc, inspector));
            exprNodeDesc = children.get(1);
            concatNode.add(ExpressionUtils.createNode(exprNodeDesc, inspector));
            return concatNode;
        }

        FunctionExpression functionExpression =
                new FunctionExpression(
                        TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                        "concat", children.size(), len);
        int group = children.size() / 2;
        int remainder = children.size() % 2;
        for (int i = 0; i < group; i++) {
            FunctionExpression concatNode = new FunctionExpression(
                    TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                    "concat", 2, len);
            ExprNodeDesc exprNodeDesc = children.get(i * 2);
            concatNode.add(ExpressionUtils.createNode(exprNodeDesc, inspector));
            exprNodeDesc = children.get(i * 2 + 1);
            concatNode.add(ExpressionUtils.createNode(exprNodeDesc, inspector));
            functionExpression.add(concatNode);
        }
        if (remainder != 0) {
            functionExpression.add(ExpressionUtils.createNode(children.get(children.size() - 1), inspector));
        }
        return functionExpression;
    }
}
