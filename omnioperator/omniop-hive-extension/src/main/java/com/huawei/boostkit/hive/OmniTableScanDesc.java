
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

@Explain(displayName = "Omni TableScan", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniTableScanDesc extends TableScanDesc {
    public OmniTableScanDesc(TableScanDesc tableScanDesc) {
        super(tableScanDesc.getAlias(), tableScanDesc.getVirtualCols(), tableScanDesc.getTableMetadata());
        this.setStatsAggPrefix(tableScanDesc.getStatsAggPrefix());
        this.setPartColumns(tableScanDesc.getPartColumns());
        this.setRowLimit(tableScanDesc.getRowLimit());
        this.setGatherStats(tableScanDesc.isGatherStats());
        this.setStatsReliable(tableScanDesc.isStatsReliable());
        this.setTmpStatsDir(tableScanDesc.getTmpStatsDir());
        this.setFilterExpr(tableScanDesc.getFilterExpr());
        this.setFilterObject(tableScanDesc.getFilterObject());
        this.setSerializedFilterExpr(tableScanDesc.getSerializedFilterExpr());
        this.setSerializedFilterObject(tableScanDesc.getSerializedFilterObject());
        this.setNeededColumnIDs(tableScanDesc.getNeededColumnIDs);
        this.setNeededColumns(tableScanDesc.getNeededColumns);
        this.setNeededNestedColumnPaths(tableScanDesc.getNeededNestedColumnPaths());
        this.setReferencedColumns(tableScanDesc.getReferencedColumns());
        this.setBucketFileNameMapping(tableScanDesc.getBucketFileNameMapping());
        this.setIsMetadataOnly(tableScanDesc.getIsMetadataOnly());
        this.setVectorized(tableScanDesc.getVectorized());
        this.setTableSample(tableScanDesc.getTableSample());
        this.setBucketFileNameMapping(tableScanDesc.getBucketFileNameMapping());
        this.setIncludedBuckets(tableScanDesc.getIncludeBuckets());
        this.setColumnExprMap(tableScanDesc.getColumnExprMap());
        this.setVectorDesc(tableScanDesc.getVectorDesc());
        this.setStatistics(tableScanDesc.getStatictics());
        this.setOpProps(tableScanDesc.getOpProps());
        this.setMemoryNeeded(tableScanDesc.getMemoryNeeded());
        this.setMaxMemoryAvailable(tableScanDesc.getMaxMemoryAvailable());
        this.setRuntimeStatsTmpDir(tableScanDesc.getRuntimeStatsTmpDir());
    }
}