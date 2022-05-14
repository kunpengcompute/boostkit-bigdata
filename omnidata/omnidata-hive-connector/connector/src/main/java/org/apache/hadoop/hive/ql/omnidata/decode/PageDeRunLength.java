/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.hadoop.hive.ql.omnidata.decode;

import org.apache.hadoop.hive.ql.exec.vector.*;

/**
 * DeCompress RunLength
 *
 * @since 2021-09-27
 */
public class PageDeRunLength {

    private final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;

    /**
     * decompress byteColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of byteArray to decompress
     * @return decompressed byteColumnVectors
     */
    public ColumnVector[] decompressByteArray(int positionCount, ColumnVector tmpColumnVector) {
        LongColumnVector tmpLongColumnVector = (LongColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpLongColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.isNull[j] = true;
                }
                longColumnVector.noNulls = false;
                resColumnVectors[i] = longColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.vector[j] = tmpLongColumnVector.vector[0];
                    longColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = longColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress booleanColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of booleanArray to decompress
     * @return decompressed booleanColumnVectors
     */
    public ColumnVector[] decompressBooleanArray(int positionCount, ColumnVector tmpColumnVector) {
        return decompressByteArray(positionCount, tmpColumnVector);
    }

    /**
     * decompress intColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of intArray to decompress
     * @return decompressed intColumnVectors
     */
    public ColumnVector[] decompressIntArray(int positionCount, ColumnVector tmpColumnVector) {
        LongColumnVector tmpLongColumnVector = (LongColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpLongColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.isNull[j] = true;
                }
                longColumnVector.noNulls = false;
                resColumnVectors[i] = longColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.vector[j] = tmpLongColumnVector.vector[0];
                    longColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = longColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress shortColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of shortArray to decompress
     * @return decompressed shortColumnVectors
     */
    public ColumnVector[] decompressShortArray(int positionCount, ColumnVector tmpColumnVector) {
        LongColumnVector tmpLongColumnVector = (LongColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpLongColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.isNull[j] = true;
                }
                longColumnVector.noNulls = false;
                resColumnVectors[i] = longColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.vector[j] = tmpLongColumnVector.vector[0];
                    longColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = longColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress longColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of longArray to decompress
     * @return decompressed longColumnVectors
     */
    public ColumnVector[] decompressLongArray(int positionCount, ColumnVector tmpColumnVector) {
        LongColumnVector tmpLongColumnVector = (LongColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpLongColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.isNull[j] = true;
                }
                longColumnVector.noNulls = false;
                resColumnVectors[i] = longColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    longColumnVector.vector[j] = tmpLongColumnVector.vector[0];
                    longColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = longColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress floatColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of floatArray to decompress
     * @return decompressed floatColumnVectors
     */
    public ColumnVector[] decompressFloatArray(int positionCount, ColumnVector tmpColumnVector) {
        DoubleColumnVector tmpDoubleColumnVector = (DoubleColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpDoubleColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    doubleColumnVector.isNull[j] = true;
                }
                doubleColumnVector.noNulls = false;
                resColumnVectors[i] = doubleColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    doubleColumnVector.vector[j] = tmpDoubleColumnVector.vector[0];
                    doubleColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = doubleColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress doubleColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of doubleArray to decompress
     * @return decompressed doubleColumnVectors
     */
    public ColumnVector[] decompressDoubleArray(int positionCount, ColumnVector tmpColumnVector) {
        DoubleColumnVector tmpDoubleColumnVector = (DoubleColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpDoubleColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    doubleColumnVector.isNull[j] = true;
                }
                doubleColumnVector.noNulls = false;
                resColumnVectors[i] = doubleColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    doubleColumnVector.vector[j] = tmpDoubleColumnVector.vector[0];
                    doubleColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = doubleColumnVector;
            }
        }
        return resColumnVectors;
    }

    /**
     * decompress stringColumnVector
     *
     * @param positionCount the positionCount to decompress
     * @param tmpColumnVector the columnVector of string to decompress
     * @return decompressed stringColumnVectors
     */
    public ColumnVector[] decompressVariableWidth(int positionCount, ColumnVector tmpColumnVector) {
        BytesColumnVector tmpBytesColumnVector = (BytesColumnVector) tmpColumnVector;
        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        ColumnVector[] resColumnVectors = new ColumnVector[batchCount];
        if (tmpBytesColumnVector.isNull[0]) {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                BytesColumnVector bytesColumnVector = new BytesColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    bytesColumnVector.isNull[j] = true;
                }
                bytesColumnVector.noNulls = false;
                resColumnVectors[i] = bytesColumnVector;
            }
        } else {
            for (int i = 0; i < batchCount; i++) {
                if (batchCount - 1 == i) {
                    loopPositionCount = remainderCount;
                }
                BytesColumnVector bytesColumnVector = new BytesColumnVector(loopPositionCount);
                for (int j = 0; j < loopPositionCount; j++) {
                    bytesColumnVector.vector[j] = tmpBytesColumnVector.vector[0];
                    bytesColumnVector.isNull[j] = false;
                }
                resColumnVectors[i] = bytesColumnVector;
            }
        }
        return resColumnVectors;
    }
}
