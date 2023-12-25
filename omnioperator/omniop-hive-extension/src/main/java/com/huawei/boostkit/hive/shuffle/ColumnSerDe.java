package com.huawei.boostkit.hive.shuffle;

public interface ColumnSerDe {
    int serialize(byte[] writeBytes, VecWrapper vecWrapper, int start);

    int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int start);
}
