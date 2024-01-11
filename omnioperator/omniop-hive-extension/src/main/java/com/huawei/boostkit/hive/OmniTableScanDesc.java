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
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

@Explain(displayName = "Omni TableScan", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniTableScanDesc extends TableScanDesc {
    public OmniTableScanDesc(TableScanDesc tableScanDesc) {
        super(tableScanDesc.getAlias(), tableScanDesc.getVirtualCols(), tableScanDesc.getTableMetadata());
        this.setStatsAggPrefix(tableScanDesc.getStatsAggPrefix());
        this.setPartColumns(tableScanDesc.getPartColumns());
        this.setRowLimit(tableScanDesc.getRowLimit());
        this.setGatherStats(tableScanDesc.isGatherStats());
        this.setStatsReliable(tableScanDesc.isStatsReliable());
        this.setTmpStatsDir(tableScanDesc.getTmpStatsDir());
        this.setFilterExpr(tableScanDesc.getFilterExpr());
        this.setFilterObject(tableScanDesc.getFilterObject());
        this.setSerializedFilterExpr(tableScanDesc.getSerializedFilterExpr());
        this.setSerializedFilterObject(tableScanDesc.getSerializedFilterObject());
        this.setNeededColumnIDs(tableScanDesc.getNeededColumnIDs());
        this.setNeededColumns(tableScanDesc.getNeededColumns());
        this.setNeededNestedColumnPaths(tableScanDesc.getNeededNestedColumnPaths());
        this.setReferencedColumns(tableScanDesc.getReferencedColumns());
        this.setBucketFileNameMapping(tableScanDesc.getBucketFileNameMapping());
        this.setIsMetadataOnly(tableScanDesc.getIsMetadataOnly());
        this.setVectorized(tableScanDesc.isVectorized());
        this.setTableSample(tableScanDesc.getTableSample());
        this.setBucketFileNameMapping(tableScanDesc.getBucketFileNameMapping());
        this.setIncludedBuckets(tableScanDesc.getIncludedBuckets());
        this.setColumnExprMap(tableScanDesc.getColumnExprMap());
        this.setVectorDesc(tableScanDesc.getVectorDesc());
        this.setStatistics(tableScanDesc.getStatistics());
        this.setOpProps(tableScanDesc.getOpProps());
        this.setMemoryNeeded(tableScanDesc.getMemoryNeeded());
        this.setMaxMemoryAvailable(tableScanDesc.getMaxMemoryAvailable());
        this.setRuntimeStatsTmpDir(tableScanDesc.getRuntimeStatsTmpDir());
    }
}