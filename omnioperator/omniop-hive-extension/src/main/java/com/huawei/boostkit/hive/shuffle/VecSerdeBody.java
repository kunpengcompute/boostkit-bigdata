package com.huawei.boostkit.hive.shuffle;

public class VecSerdeBody {
    public byte[] value;

    public byte isNull;

    public int length;

    public VecSerdeBody(int size) {
        value = new byte[size];
    }
}
