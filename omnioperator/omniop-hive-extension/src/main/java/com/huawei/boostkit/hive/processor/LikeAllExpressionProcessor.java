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
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class LikeAllExpressionProcessor extends LikeExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        boolean isNot = operator.equals("NOT");
        List<ExprNodeDesc> children = node.getChildren();
        int group = (children.size() - 1) / 2;
        BaseExpression compareGroup = null;
        if (group > 0) {
            compareGroup = new CompareExpression("BINARY",
                    TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                    isNot ? "AND" : operator);
        }
        for (int i = 1; i < children.size(); i++) {
            BaseExpression like = buildLikeNode(node, i, inspector);
            if (isNot) {
                like = wrapNotExpression(like);
            }
            if (compareGroup != null) {
                compareGroup.add(like);
            } else {
                compareGroup = like;
            }
        }
        return compareGroup;
    }
}
