package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class ColumnCache {
    public boolean[] isNull;

    public boolean noNulls = true;

    public ColumnCache() {
        isNull = new boolean[BATCH];
    }

    public void reset() {
        noNulls = true;
        isNull = new boolean[BATCH];
    }
}
