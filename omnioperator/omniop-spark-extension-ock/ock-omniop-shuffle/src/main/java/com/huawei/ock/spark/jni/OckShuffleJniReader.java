/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.ock.spark.jni;

import nova.hetu.omniruntime.vector.Vec;

import java.rmi.UnexpectedException;
import java.util.logging.Logger;

/**
 * OckShuffleJniReader.
 *
 * @since 2022-6-10
 */
public class OckShuffleJniReader {
    private static final Logger logger = Logger.getLogger(OckShuffleJniReader.class.getName());

    private long blobId = 0L;
    private long capacity = 0L;
    private long baseAddress = 0L; // read blob native base address
    private int totalReadBytes = 0;
    private long currentVBDataAddr = 0L;
    private int currentVBLength = 0; // Byte
    private boolean isLastVB = false;
    private long nativeReader = 0L;
    private long valueLen;
    private int rowCntCurrent = 0;
    private int colCnt = 0;

    /**
     * OckShuffleJniReader constructor
     */
    public OckShuffleJniReader() {
        NativeLoader.getInstance();
    }

    /**
     * OckShuffleJniReader constructor
     *
     * @param blobId blobId
     * @param capacity capacity
     * @param baseAddress baseAddress
     * @param valueLen value length
     * @param typeIds typeIds
     */
    public OckShuffleJniReader(long blobId, int capacity, long baseAddress, long valueLen, int[] typeIds) {
        this();
        this.blobId = blobId;
        this.capacity = capacity;
        this.baseAddress = baseAddress;
        this.currentVBDataAddr = baseAddress;
        this.nativeReader = make(typeIds);
        if (valueLen >= 0L && valueLen <= this.capacity) {
            this.valueLen = valueLen;
        } else {
            throw new IllegalArgumentException();
        }

        this.colCnt = typeIds.length;
    }

    public final long getValueLen() {
        return this.valueLen;
    }

    /**
     * update value length
     *
     * @param newLim newLength
     * @return OckShuffleJniReader
     */
    public final OckShuffleJniReader upgradeValueLen(long newLim) {
        if (newLim >= 0L && newLim <= this.capacity) {
            currentVBDataAddr = baseAddress;
            currentVBLength = 0;
            totalReadBytes = 0;
            isLastVB = false;
            valueLen = newLim;
            rowCntCurrent = 0;
            return this;
        } else {
            logger.warning("arg newlim is illegal");
            throw new IllegalArgumentException();
        }
    }

    public boolean readFinish() {
        return isLastVB;
    }

    /**
     * get new vectorBatch
     *
     * @param maxLength maxLength
     * @param maxRowNum maxRowNum
     * @throws UnexpectedException UnexpectedException
     */
    public void getNewVectorBatch(int maxLength, int maxRowNum) throws UnexpectedException {
        Long rowCnt = 256L;
        currentVBDataAddr += currentVBLength; // skip to last vb

        currentVBLength = nativeGetVectorBatch(nativeReader, currentVBDataAddr,
            (int) (valueLen - totalReadBytes), maxRowNum, maxLength, rowCnt);
        if (currentVBLength <= 0) {
            throw new UnexpectedException("Failed to get native vector batch for blobId "
                + this.blobId + ", length " + "is " + currentVBLength);
        }

        rowCntCurrent = rowCnt.intValue();
        totalReadBytes += currentVBLength;

        if (totalReadBytes > this.valueLen) {
            throw new UnexpectedException("The bytes already read exceed blob ("
                    + blobId + ") size (" + totalReadBytes + " > " + this.valueLen + ")");
        }

        if (totalReadBytes == this.valueLen) {
            isLastVB = true;
        }
    }

    public int rowCntInVB() {
        return rowCntCurrent;
    }

    public int colCntInVB() {
        return colCnt;
    }

    /**
     * get vector value length.
     *
     * @param colIndex colIndex
     * @return vector value length
     */
    public int getVectorValueLength(int colIndex) {
        // length in bytes of the vector data
        return nativeGetVecValueLength(nativeReader, colIndex);
    }

    /**
     * copy vector data in vectorBatch.
     *
     * @param dstVec dstVec
     * @param colIndex colIndex
     */
    public void copyVectorDataInVB(Vec dstVec, int colIndex) {
        nativeCopyVecDataInVB(nativeReader, dstVec.getNativeVector(), colIndex);
    }

    /**
     * close reader.
     *
     */
    public void doClose() {
        close(nativeReader);
    }

    private native long make(int[] typeIds);

    private native long close(long readerId);

    private native int nativeGetVectorBatch(long readerId, long vbDataAddr, int capacity, int maxRow,
        int maxDataSize, Long rowCnt);

    private native int nativeGetVecValueLength(long readerId, int colIndex);

    private native void nativeCopyVecDataInVB(long readerId, long dstNativeVec, int colIndex);
}