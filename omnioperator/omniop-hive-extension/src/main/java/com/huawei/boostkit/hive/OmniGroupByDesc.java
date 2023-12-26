package com.huawei.boostkit.hive;


import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;

@Explain(displayName = "Omni Group By Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED})
public class OmniGroupByDesc extends GroupByDesc {
    public OmniGroupByDesc(GroupByDesc groupByDesc) {
        super(groupByDesc.getMode(), groupByDesc.getOutputColumnNames(), groupByDesc.getKeys(),
                groupByDesc.getAggregators(), groupByDesc.getGroupByMemoryUsage(), groupByDesc.getMemoryThreshold(),
                groupByDesc.getListGroupingSets(), groupByDesc.isGroupingSetsPresent(),
                groupByDesc.getGroupingSetPosition(), groupByDesc.isDistinct());
        this.vectorDesc = groupByDesc.getVectorDesc();
    }
}
