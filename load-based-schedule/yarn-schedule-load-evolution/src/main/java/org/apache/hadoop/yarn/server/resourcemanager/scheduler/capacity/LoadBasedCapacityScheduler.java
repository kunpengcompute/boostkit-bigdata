/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public class LoadBasedCapacityScheduler extends CapacityScheduler {
    public LoadBasedCapacityScheduler() {
        super(LoadBasedCapacityScheduler.class.getName());
    }
}