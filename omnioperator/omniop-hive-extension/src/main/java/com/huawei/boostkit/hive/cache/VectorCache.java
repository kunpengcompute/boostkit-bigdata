package com.huawei.boostkit.hive.cache;

import static org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch.DEFAULT_SIZE;

public class VectorCache {
    public static final int BATCH = 4 * DEFAULT_SIZE;

    public Object[][] dataCache;

    public VectorCache() {
    }

    public VectorCache(int colNum) {
        dataCache = new Object[colNum][BATCH];
    }
}
