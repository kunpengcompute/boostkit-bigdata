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
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class CastExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        TypeInfo typeInfo = node.getTypeInfo();
        Integer precision = null;
        Integer scale = null;
        if (node.getTypeInfo() instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) node.getTypeInfo()).getPrecision();
            scale = ((DecimalTypeInfo) node.getTypeInfo()).getScale();
        }

        int width = TypeUtils.getCharWidth(node);

        CastFunctionExpression castFunctionExpression = new CastFunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(typeInfo),
                width,
                precision,
                scale);
        List<ExprNodeDesc> children = node.getChildren();
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                castFunctionExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else {
                castFunctionExpression.add(ExpressionUtils.createNode(child, inspector));
            }
        }
        return castFunctionExpression;
    }
}
