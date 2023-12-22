
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

@Explain(displayName = "Omni Map Join Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniMapJoinDesc extends MapJoinDesc {
    public OmniMapJoinDesc(MapJoinDesc mapJoinDesc) {
        super(mapJoinDesc);
        setValueFilteredTblDescs(mapJoinDesc.getValueFilteredTblDescs());
        setDynamicPartitionHashJoin(mapJoinDesc.isDynamicPartitionHashJoin());
    }
}
