/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.service;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.entity.UsageMetrics;

import java.util.Map;

public interface UsageMetricsService {

    String getUsageMetricsForPrometheus();

    UsageMetrics calcuUsageMetrics(Map<String, FixedSizeRingBuffer<LoadsRecords>> loadRecordsCache);

}
