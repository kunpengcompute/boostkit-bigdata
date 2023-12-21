package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class LongColumnCache extends ColumnCache{
    public final long[] dataCache;

    public LongColumnCache(){
        dataCache=new long[BATCH];
    }
}
