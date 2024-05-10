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
import com.huawei.boostkit.hive.expression.BetweenExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
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
        if (node.getChildren().get(1) instanceof ExprNodeGenericFuncDesc) {
            subLeaf = ExpressionUtils.build((ExprNodeGenericFuncDesc) comparedNode, inspector);
        } else {
            subLeaf = ExpressionUtils.createNode(comparedNode, inspector);
        }
        return new BetweenExpression("BETWEEN", TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                null, createBoundNode(node, comparedNode, LOWER_BOUND_INDEX, inspector),
                createBoundNode(node, comparedNode, UPPER_BOUND_INDEX, inspector), subLeaf);
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
