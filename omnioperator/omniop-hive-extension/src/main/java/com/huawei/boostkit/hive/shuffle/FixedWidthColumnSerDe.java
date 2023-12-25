package com.huawei.boostkit.hive.shuffle;

public class FixedWidthColumnSerDe implements ColumnSerDe {
    private int columnTypeLen;
    private int length;
    private static byte[] EMPTY = new byte[16];

    public FixedWidthColumnSerDe(int columnTypeLen) {
        this.columnTypeLen = columnTypeLen;
    }

    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int index = vecWrapper.index;
        if (vecWrapper.isNull[index] == 1) {
            writeBytes[offset] = -1;
            ++offset;
            return offset;
        }
        int valueLen = trimBytes(vecWrapper.value, index * columnTypeLen, columnTypeLen);
        writeBytes[offset] = (byte) valueLen;
        ++offset;
        // write value array
        System.arraycopy(vecWrapper.value, index * columnTypeLen, writeBytes, offset, valueLen);
        offset = offset + valueLen;
        return offset;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        if (bytes[offset] == -1) {
            vecSerdeBody.isNull = 1;
            ++offset;
            System.arraycopy(EMPTY, 0, vecSerdeBody.value, 0, columnTypeLen);
            return offset;
        }
        vecSerdeBody.isNull = 0;
        length = bytes[offset];
        ++offset;
        System.arraycopy(bytes, offset, vecSerdeBody.value, 0, length);
        System.arraycopy(EMPTY, 0, vecSerdeBody.value, length, columnTypeLen - length);
        return offset;
    }

    private int trimBytes(byte[] bytes, int start, int length) {
        int index = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[start + i] != 0) {
                index = i;
                break;
            }
        }
        return index + 1;
    }
}
