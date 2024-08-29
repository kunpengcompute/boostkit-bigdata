/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.controller;

import com.huawei.bdsolution.loadsmetric.service.UsageMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/loadIndicator")
@CrossOrigin
@PropertySource("${SPRING_CONFIG_LOCATION}")
public class LoadIndicatorController {
    @Autowired
    UsageMetricsService usageMetricsService;

    @GetMapping("/get/byCurrTimeForPrometheus")
    public String getUsageMetricsForPrometheus() {
        return usageMetricsService.getUsageMetricsForPrometheus();
    }

}
