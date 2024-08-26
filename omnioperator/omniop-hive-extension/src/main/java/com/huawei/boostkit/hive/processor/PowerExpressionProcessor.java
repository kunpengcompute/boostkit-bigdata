/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class PowerExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        FunctionExpression functionExpression = new FunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()), "power", children.size(), null);
        for (ExprNodeDesc child : children) {
            if (child.getTypeInfo().getTypeName().equals("double")) {
                if (child instanceof ExprNodeGenericFuncDesc) {
                    functionExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
                } else {
                    functionExpression.add(ExpressionUtils.createNode(child, inspector));
                }
            } else {
                PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
                typeInfo.setTypeName("double");
                GenericUDFBridge genericUDFBridge = new GenericUDFBridge("UDFToDouble", false, "org.apache.hadoop.hive.ql.udf.UDFToDouble");
                List<ExprNodeDesc> exprNodeDescList = new ArrayList<>();
                exprNodeDescList.add(child);
                ExprNodeGenericFuncDesc child2 = new ExprNodeGenericFuncDesc(typeInfo, genericUDFBridge, exprNodeDescList);
                functionExpression.add(ExpressionUtils.build(child2, inspector));
            }
        }
        return functionExpression;
    }
}
