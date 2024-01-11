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

package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Explain(displayName = "Omni Merge Join Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniMergeJoinDesc extends JoinDesc {
    private Map<Byte, List<ExprNodeDesc>> keys;

    public OmniMergeJoinDesc(JoinDesc joinDesc) {
        super(joinDesc);
    }

    public OmniMergeJoinDesc(CommonMergeJoinDesc commonMergeJoinDesc) {
        this((JoinDesc) commonMergeJoinDesc);
        keys = commonMergeJoinDesc.getKeys();
    }

    @Override
    @Explain(displayName = "keys")
    public Map<String, String> getKeysString() {
        Map<String, String> keyMap = new LinkedHashMap<>();
        for (Map.Entry<Byte, List<ExprNodeDesc>> k : keys.entrySet()) {
            keyMap.put(String.valueOf(k.getKey()), PlanUtils.getExprListString(k.getValue()));
        }
        return keyMap;
    }

    @Override
    @Explain(displayName = "keys", explainLevels = {Explain.Level.USER})
    public Map<Byte, String> getUserLevelExplainKeysString() {
        Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
        for (Map.Entry<Byte, List<ExprNodeDesc>> k : keys.entrySet()) {
            keyMap.put(k.getKey(), PlanUtils.getExprListString(k.getValue(), true));
        }
        return keyMap;
    }
}
