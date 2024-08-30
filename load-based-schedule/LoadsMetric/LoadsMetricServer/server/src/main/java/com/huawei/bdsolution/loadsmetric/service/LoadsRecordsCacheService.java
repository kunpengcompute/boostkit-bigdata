/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.service;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.dto.NodeLogicalResource;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;

import java.util.Map;

public interface LoadsRecordsCacheService {

    void addLoadsRecordsCache(LoadsRecords loadsRecords);

    Map<String, FixedSizeRingBuffer<LoadsRecords>> getLoadsRecordsCacheMap();

    void addNodeLogicalResource(NodeLogicalResource nodeLogicalResource);

    Map<String,NodeLogicalResource> getNodeLogicalResourceMap();

    String getRecordsByCurrTimeForPrometheus();
}
