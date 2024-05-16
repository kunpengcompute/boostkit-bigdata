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
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class BetweenExpressionProcessor implements ExpressionProcessor {

    private static final int LOWER_BOUND_INDEX = 2;
    private static final int UPPER_BOUND_INDEX = 3;

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        BaseExpression subLeaf;
        ExprNodeDesc comparedNode = node.getChildren().get(1);
        if (comparedNode instanceof ExprNodeGenericFuncDesc) {
            subLeaf = ExpressionUtils.build((ExprNodeGenericFuncDesc) comparedNode, inspector);
        } else {
            subLeaf = ExpressionUtils.createNode(comparedNode, inspector);
        }
        BaseExpression lowerBoundNode = createBoundNode(node, comparedNode, LOWER_BOUND_INDEX, inspector);
        BaseExpression upperBoundNode = createBoundNode(node, comparedNode, UPPER_BOUND_INDEX, inspector);
        boolean not = (boolean) ((ExprNodeConstantDesc) node.getChildren().get(0)).getValue();
        CompareExpression leaf = new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                not ? "OR" : "AND");
        leaf.add(new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                not ? "LESS_THAN" : "GREATER_THAN_OR_EQUAL"));
        leaf.add(new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                not ? "GREATER_THAN" : "LESS_THAN_OR_EQUAL"));
        leaf.add(subLeaf);
        leaf.add(lowerBoundNode);
        leaf.add(subLeaf);
        leaf.add(upperBoundNode);
        return leaf;
    }

    private BaseExpression createBoundNode(ExprNodeGenericFuncDesc rootNode,
                                           ExprNodeDesc comparedNode, int boundIndex, ObjectInspector inspector) {
        ExprNodeDesc node = rootNode.getChildren().get(boundIndex);
        BaseExpression resultNode;
        if (node instanceof ExprNodeGenericFuncDesc) {
            resultNode = ExpressionUtils.build((ExprNodeGenericFuncDesc) node, inspector);
        } else {
            resultNode = ExpressionUtils.createNode(node, inspector);
        }
        if (!comparedNode.getTypeInfo().equals(
                rootNode.getChildren().get(boundIndex).getTypeInfo())) {
            resultNode = ExpressionUtils.preCast(resultNode, node, comparedNode);
        }
        return resultNode;
    }
}
