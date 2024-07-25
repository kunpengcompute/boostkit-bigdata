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
import com.huawei.boostkit.hive.expression.NotExpression;
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class LikeExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        return buildLikeNode(node, 1, inspector);
    }

    protected BaseExpression buildLikeNode(ExprNodeGenericFuncDesc node, int index, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        ExprNodeDesc fieldReferenceNode = children.get(0);
        BaseExpression referenceNode = ExpressionUtils.createReferenceNode(fieldReferenceNode, inspector);
        FunctionExpression like = new FunctionExpression(TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                "LIKE", children.size(), null);
        like.add(referenceNode);
        String regExp = likePatternToRegExp(((ExprNodeConstantDesc) children.get(index)).getValue().toString());
        like.add(ExpressionUtils.createLiteralNode(children.get(index)));
        return like;
    }

    protected String likePatternToRegExp(String likePattern) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < likePattern.length(); ++i) {
            char n = likePattern.charAt(i);
            if (n == '\\' && i + 1 < likePattern.length()
                    && (likePattern.charAt(i + 1) == '_' || likePattern.charAt(i + 1) == '%')) {
                sb.append(likePattern.charAt(i + 1));
                ++i;
            } else if (n == '_') {
                sb.append(".");
            } else if (n == '%') {
                sb.append(".*?");
            } else {
                sb.append(n);
            }
        }
        return sb.toString();
    }

    protected BaseExpression wrapNotExpression(BaseExpression expression) {
        NotExpression notExpression = new NotExpression();
        notExpression.setExpr(expression);
        return notExpression;
    }
}
