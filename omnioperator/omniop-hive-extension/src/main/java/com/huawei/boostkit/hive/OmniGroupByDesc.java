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


import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;

@Explain(displayName = "Omni Group By Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED})
public class OmniGroupByDesc extends GroupByDesc {
    public OmniGroupByDesc(GroupByDesc groupByDesc) {
        super(groupByDesc.getMode(), groupByDesc.getOutputColumnNames(), groupByDesc.getKeys(),
                groupByDesc.getAggregators(), groupByDesc.getGroupByMemoryUsage(), groupByDesc.getMemoryThreshold(),
                groupByDesc.getListGroupingSets(), groupByDesc.isGroupingSetsPresent(),
                groupByDesc.getGroupingSetPosition(), groupByDesc.isDistinct());
        this.vectorDesc = groupByDesc.getVectorDesc();
    }
}
