package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class BytesColumnCache extends ColumnCache {
    public final VarcharCache[] dataCache;

    public BytesColumnCache() {
        dataCache = new VarcharCache[BATCH];
    }
}
