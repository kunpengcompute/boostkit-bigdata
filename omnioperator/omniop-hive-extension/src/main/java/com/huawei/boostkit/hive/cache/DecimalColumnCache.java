package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class DecimalColumnCache extends ColumnCache{
    public final byte[][] dataCache;

    public  DecimalColumnCache(){
        dataCache=new byte[BATCH][16];
    }
}
