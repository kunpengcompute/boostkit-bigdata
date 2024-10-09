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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

import java.util.HashMap;

class OmniHiveConf {
    public boolean enableOperatorJoin;
    public boolean enableOperatorMapJoin;
    public boolean enableOperatorMergejoin;
    public boolean enableOperatorGroupby;
    public boolean enableOperatorSelect;
    public boolean enableOperatorFilter;
    public boolean enableOperatorPtf;
    public boolean enableOperatorTablescan;
    public boolean enableOperatorReducesink;
    public int stringLength;
    private HashMap<OperatorType, Boolean> operatorEnableMap;
    public boolean enableAdaptivePartialAggregation;
    public long adaptivePartialAggregationMinRows;
    public double adaptivePartialAggregationRatio;

    public OmniHiveConf(HiveConf hiveConf) {
        enableOperatorJoin = hiveConf.getBoolean("omni.hive.join.enabled", true);
        enableOperatorMapJoin = hiveConf.getBoolean("omni.hive.mapjoin.enabled", true);
        enableOperatorMergejoin = hiveConf.getBoolean("omni.hive.mergejoin.enabled", true);
        enableOperatorGroupby = hiveConf.getBoolean("omni.hive.groupby.enabled", true);
        enableOperatorSelect = hiveConf.getBoolean("omni.hive.select.enabled", true);
        enableOperatorFilter = hiveConf.getBoolean("omni.hive.filter.enabled", true);
        enableOperatorPtf = hiveConf.getBoolean("omni.hive.ptf.enabled", true);
        enableOperatorTablescan = hiveConf.getBoolean("omni.hive.tablescan.enabled", true);
        enableOperatorReducesink = hiveConf.getBoolean("omni.hive.reducesink.enabled", true);
        operatorEnableMap = new HashMap<OperatorType, Boolean>() {{
            put(OperatorType.JOIN, enableOperatorJoin);
            put(OperatorType.MAPJOIN, enableOperatorMapJoin);
            put(OperatorType.MERGEJOIN, enableOperatorMergejoin);
            put(OperatorType.GROUPBY, enableOperatorGroupby);
            put(OperatorType.SELECT, enableOperatorSelect);
            put(OperatorType.FILTER, enableOperatorFilter);
            put(OperatorType.PTF, enableOperatorPtf);
            put(OperatorType.TABLESCAN, enableOperatorTablescan);
            put(OperatorType.REDUCESINK, enableOperatorReducesink);
        }};
        stringLength = hiveConf.getInt("omni.hive.string.length", 1024);
        enableAdaptivePartialAggregation = hiveConf.getBoolean("omni.hive.adaptivePartialAggregation.enabled", false);
        adaptivePartialAggregationMinRows = hiveConf.getLong("omni.hive.adaptivePartialAggregationMinRows", 10000);
        adaptivePartialAggregationRatio = hiveConf.getDouble("omni.hive.adaptivePartialAggregationRatio", 0.8);
    }

    public boolean isEnableOperator(OperatorType operatorType) {
        return operatorEnableMap.get(operatorType);
    }
}