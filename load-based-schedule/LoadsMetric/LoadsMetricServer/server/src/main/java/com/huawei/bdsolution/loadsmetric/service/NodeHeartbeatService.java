/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.service;

import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;

import java.util.Set;

public interface NodeHeartbeatService {

    public void initAllNodeHosts();

    public void updateNodeHeartbeatTime(LoadsRecords loadsRecords);

    public void reportLostNodes();

    public Set<String> getAllNodeHosts();
}
