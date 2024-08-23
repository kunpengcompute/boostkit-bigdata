package com.huawei.bdsolution.loadsmetric.controller;


import com.huawei.bdsolution.loadsmetric.dto.SortReport;
import com.huawei.bdsolution.loadsmetric.service.LoadsRecordsCacheService;
import com.huawei.bdsolution.loadsmetric.service.SortReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/scheduler")
@CrossOrigin
public class SchedulerController {

    @Autowired
    SortReportService sortReportService;

    @Autowired
    LoadsRecordsCacheService loadsRecordsCacheService;

    @GetMapping("/sortWithLogical")
    public SortReport findAverageResourceUsageForLastSecondsWithLogical(){
        return sortReportService.sortNodesByUsagesWithLogicalFromCache(
                loadsRecordsCacheService.getLoadsRecordsCacheMap(),
                loadsRecordsCacheService.getNodeLogicalResourceMap());
    }
}
