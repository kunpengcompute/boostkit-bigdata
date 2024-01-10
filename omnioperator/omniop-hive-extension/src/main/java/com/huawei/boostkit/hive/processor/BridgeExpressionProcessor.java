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
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class BridgeExpressionProcessor implements ExpressionProcessor {
    public static final List<String> SUPPORT_BRIDGE_UDF = new ArrayList<String>() {
        {
            add("UDFToDouble");
            add("UDFToInteger");
            add("UDFToLong");
            add("UDFToShort");
            add("UDFToByte");
            add("UDFToBoolean");
            add("substr");
        }
    };

    private boolean simplify = false;

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        BaseExpression leaf = null;
        switch (node.getGenericUDF().getUdfName()) {
            case "UDFToString":
                if (node.getChildren().get(0).getTypeInfo() instanceof BaseCharTypeInfo) {
                    leaf = createNode(node.getChildren().get(0), inspector);
                }
                break;
            case "like":
                LikeExpressionProcessor likeExpressionProcessor = new LikeExpressionProcessor();
                leaf = likeExpressionProcessor.process(node, operator, inspector);
                break;
            case "substr":
                if (simplify) {
                    SimplifySubstrExpressionProcessor simplifySubstrExpressionProcessor =
                            new SimplifySubstrExpressionProcessor();
                    leaf = simplifySubstrExpressionProcessor.process(node, operator, inspector);
                } else {
                    SubstrExpressionProcessor substrExpressionProcessor = new SubstrExpressionProcessor();
                    leaf = substrExpressionProcessor.process(node, operator, inspector);
                }
                break;
            case "UDFToDouble":
            case "UDFToInteger":
            case "UDFToLong":
            case "UDFToShort":
            case "UDFToByte":
            case "UDFToBoolean":
                leaf = createNode(node.getChildren().get(0), inspector);
                CastFunctionExpression functionExpression = new CastFunctionExpression(
                        TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()), null, null, null);
                return ExpressionUtils.optimizeCast(leaf, functionExpression);
            default:
                break;
        }
        return leaf;
    }

    private BaseExpression createNode(ExprNodeDesc nodeDesc, ObjectInspector inspector) {
        if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            return ExpressionUtils.build((ExprNodeGenericFuncDesc) nodeDesc, inspector);
        } else {
            return ExpressionUtils.createNode(nodeDesc, inspector);
        }
    }

    public void setSimplify(boolean simplify) {
        this.simplify = simplify;
    }
}
