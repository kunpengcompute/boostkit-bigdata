package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.FilterDesc;

@Explain(displayName = "Omni Filter Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED})
public class OmniFilterDesc extends FilterDesc {
    public OmniFilterDesc(FilterDesc filterDesc) {
        super(filterDesc.getPredicate(), filterDesc.getIsSamplingPred(), filterDesc.getSampleDescr());
        this.setGenerated(filterDesc.isGenerated());
        this.setSortedFilter(filterDesc.isSortedFilter());
        this.setSyntheticJoinPredicate(filterDesc.isSyntheticJoinPredicate());
    }
}
