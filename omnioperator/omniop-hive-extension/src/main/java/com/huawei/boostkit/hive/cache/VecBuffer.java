package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

public class VecBuffer {
    public byte[] byteBuffer;
    public byte[] isNull;
    public int[] offset;
    public boolean noNulls;
    public boolean isChar;

    public VecBuffer(int colLength, boolean isChar) {
        this.isChar = isChar;
        offset = new int[BATCH + 1];

        byteBuffer = new byte[colLength * BATCH];
        isNull = new byte[BATCH];
        if (!isChar) {
            for (int i = 0; i < offset.length; i++) {
                offset[i] = colLength * i;
            }
        }
    }
}
