
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

@Explain(displayName = "Omni ReduceSink", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniReduceSinkDesc extends ReduceSinkDesc {
    public OmniReduceSinkDesc(ReduceSinkDesc reduceSinkDesc) {
        super(reduceSinkDesc.getKeyCols(), reduceSinkDesc.getNumDistributionKeys(), reduceSinkDesc.getValueCols(), reduceSinkDesc.getOutputKeyColumnNames(),
                reduceSinkDesc.getDistinctColumnIndices(), reduceSinkDesc.getOutputValueColumnNames(), reduceSinkDesc.getTag(), reduceSinkDesc.getPartitionCols(),
                reduceSinkDesc.getNumReducers(), reduceSinkDesc.getKeySerializeInfo(), reduceSinkDesc.getValueSerializeInfo(), reduceSinkDesc.getWriteType());
        this.setSkipTag(reduceSinkDesc.getSkipTag());
        this.setTopN(reduceSinkDesc.getTopN());
        this.setTopNMemoryUsage(reduceSinkDesc.getTopNMemoryUsage());
        this.setPTFReduceSink(reduceSinkDesc.isPTFReduceSink());
        this.setMapGroupBy(reduceSinkDesc.isMapGroupBy());
        this.setReducerTraits(reduceSinkDesc.getReducerTraits());
        this.setBucketCols(reduceSinkDesc.getBucketCols());
        this.setColumnExprMap(reduceSinkDesc.getColumnExprMap());
        this.setDeduplicated(reduceSinkDesc.isDeduplicated());
        this.setForwarding(reduceSinkDesc.isForwarding());
        this.setHasOrderBy(reduceSinkDesc.hasOrderBy());
        this.setMaxMemoryAvailable(reduceSinkDesc.getMaxMemoryAvailable());
        this.setMemoryNeeded(reduceSinkDesc.getMemoryNeeded());
        this.setNullOrder(reduceSinkDesc.getNullOrder());
        this.setNumBuckets(reduceSinkDesc.getNumBuckets());
        this.setNumDistributionKeys(reduceSinkDesc.getNumDistributionKeys());
        this.setOpProps(reduceSinkDesc.getOpProps());
        this.setOrder(reduceSinkDesc.getOrder());
        this.setRuntimeStatsTmpDir(reduceSinkDesc.getRuntimeStatsTmpDir());
        this.setStatistics(reduceSinkDesc.getStatistics());
        this.setOutputName(reduceSinkDesc.getOutputName());
        this.setOutputKeyColumnNames(reduceSinkDesc.getOutputKeyColumnNames());
        this.setOutputValueColumnNames(reduceSinkDesc.getOutputValueColumnNames());
    }
}
