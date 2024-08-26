package com.huawei.bdsolution.loadsmetric.service;

import com.huawei.bdsolution.loadsmetric.entity.LoadsRecords;

import java.util.Set;

public interface NodeHeartbeatService {

    public void initAllNodeHosts();

    public void updateNodeHeartbeatTime(LoadsRecords loadsRecords);

    public void reportLostNodes();

    public Set<String> getAllNodeHosts();
}
