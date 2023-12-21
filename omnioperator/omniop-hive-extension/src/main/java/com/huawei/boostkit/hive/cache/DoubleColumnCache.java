package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class DoubleColumnCache extends ColumnCache{
    public final double[] dataCache;
    public DoubleColumnCache(){
        dataCache=new double[BATCH];
    }
}
