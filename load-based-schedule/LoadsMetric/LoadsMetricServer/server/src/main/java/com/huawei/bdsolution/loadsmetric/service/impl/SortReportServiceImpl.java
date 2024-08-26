package com.huawei.bdsolution.loadsmetric.service.impl;

import com.huawei.bdsolution.loadsmetric.dto.FixedSizeRingBuffer;
import com.huawei.bdsolution.loadsmetric.dto.LoadsRecordAverage;
import com.huawei.bdsolution.loadsmetric.dto.NodeLogicalResource;
import com.huawei.bdsolution.loadsmetric.dto.SortReport;
import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;
import com.huawei.bdsolution.loadsmetric.service.SortReportService;
import com.huawei.bdsolution.loadsmetric.util.MultiLoadResourceUsageSortPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SortReportServiceImpl implements SortReportService {

    @Autowired
    MultiLoadResourceUsageSortPolicy multiLoadResourceUsageSortPolicy;

    @Override
    public SortReport sortNodesByUsagesWithLogicalFromCache(
            Map<String, FixedSizeRingBuffer<LoadsRecords>> loadsRecordsCacheMap,
            Map<String, NodeLogicalResource> nodeLogicalResourceMap) {
        List<LoadsRecordAverage> loadsRecordAverageList = multiLoadResourceUsageSortPolicy.calculateAverage(loadsRecordsCacheMap);
        multiLoadResourceUsageSortPolicy.calculateWeight(loadsRecordAverageList);
        return multiLoadResourceUsageSortPolicy.sortNodesWithLogical(loadsRecordAverageList,nodeLogicalResourceMap);
    }
}
