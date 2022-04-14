package com.huawei.boostkit.hbase.index;

public interface OffheapReplaceableCache {
    void retain();
    void release();
    int dataSize();
    OffheapReplaceableCache replace(AllocatedMemory allocate);
}
