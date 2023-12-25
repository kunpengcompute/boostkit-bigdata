package com.huawei.boostkit.hive.shuffle;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;

public class VariableWidthColumnSerDe implements ColumnSerDe {
    private byte[] lengthBytes = new byte[4];
    private LazyBinaryUtils.VInt vInt = new LazyBinaryUtils.VInt();


    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int index = vecWrapper.index;
        if (vecWrapper.isNull[index] == 1) {
            writeBytes[offset] = -1;
            ++offset;
            return offset;
        }
        // write length
        int valueLen = vecWrapper.offset[index + 1] - vecWrapper.offset[index];
        int len = LazyBinaryUtils.writeVLongToByteArray(lengthBytes, valueLen);
        System.arraycopy(lengthBytes, 0, writeBytes, offset, len);
        offset = offset + len;
        // write value array
        System.arraycopy(vecWrapper.value, vecWrapper.offset[index], writeBytes, offset, valueLen);
        offset = offset + valueLen;
        return offset;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        if (bytes[offset] == -1) {
            vecSerdeBody.isNull = 1;
            vecSerdeBody.length = 0;
            ++offset;
            return offset;
        }
        vecSerdeBody.isNull = 0;
        LazyBinaryUtils.readVInt(bytes, offset, vInt);
        vecSerdeBody.length = vInt.value;
        System.arraycopy(bytes, offset + vInt.length, vecSerdeBody.value, 0, vInt.value);
        offset = offset + vInt.length + vInt.value;
        return offset;
    }
}
