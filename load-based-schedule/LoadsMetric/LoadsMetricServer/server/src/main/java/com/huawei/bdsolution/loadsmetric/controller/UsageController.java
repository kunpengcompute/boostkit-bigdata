/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.controller;

import com.huawei.bdsolution.loadsmetric.dto.NodeLogicalResource;
import com.huawei.bdsolution.loadsmetric.service.LoadsRecordsCacheService;
import com.huawei.bdsolution.loadsmetric.service.NodeExporterService;
import com.huawei.bdsolution.loadsmetric.service.NodeHeartbeatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/loadStatus")
@CrossOrigin
@PropertySource("${SPRING_CONFIG_LOCATION}")
public class UsageController {

    @Autowired
    NodeHeartbeatService nodeHeartbeatService;

    @Autowired
    LoadsRecordsCacheService loadsRecordsCacheService;

    @Autowired
    NodeExporterService nodeExporterService;

    @PostMapping("/addNodeLogicalResource")
    public void receiveNodeLogicalResource(@RequestBody NodeLogicalResource nodeLogicalResource) {
        loadsRecordsCacheService.addNodeLogicalResource(nodeLogicalResource);
    }

    @GetMapping("/allNodes")
    public List<String> findAllNodes() {
        return new ArrayList<>(nodeHeartbeatService.getAllNodeHosts());
    }

    @GetMapping("/get/byCurrTimeForPrometheus")
    public String getRecordsByCurrTimeForPrometheus() {
        return loadsRecordsCacheService.getRecordsByCurrTimeForPrometheus();
    }
}
