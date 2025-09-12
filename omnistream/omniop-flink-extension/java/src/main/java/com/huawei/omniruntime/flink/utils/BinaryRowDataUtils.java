package com.huawei.omniruntime.flink.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BinaryRowDataUtils
 *
 * @since 2025-04-27
 */
public class BinaryRowDataUtils {
    /**
     * LOG
     */
    protected static final Logger LOG = LoggerFactory.getLogger(BinaryRowDataUtils.class);

    /**
     * hexContentBinaryRowData
     *
     * @param binaryRowData binaryRowData
     * @return String
     */
    public static String hexContentBinaryRowData(BinaryRowData binaryRowData) {
        MemorySegment[] segments = binaryRowData.getSegments();
        int offset = binaryRowData.getOffset();
        int sizInBytes = binaryRowData.getSizeInBytes();

        // assume there is only one segement
        MemorySegment segment = segments[0];
        int size = segment.size();
        int printSize = offset + sizInBytes > size ? size - offset : sizInBytes;
        byte[] bytes = new byte[printSize];
        segment.get(offset, bytes, 0, printSize);

        return HexConverter.bytesToHex(bytes);
    }
}
