/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.ock.spark.jni;

import com.huawei.boostkit.spark.vectorized.PartitionInfo;
import com.huawei.boostkit.spark.vectorized.SplitResult;

import java.rmi.UnexpectedException;

/**
 * OckShuffleJniWriter.
 *
 * @since 2022-6-10
 */
public class OckShuffleJniWriter {
    /**
     * OckShuffleJniWriter constructor.
     *
     * @throws UnexpectedException UnexpectedException
     */
    public OckShuffleJniWriter() throws UnexpectedException {
        NativeLoader.getInstance();
        boolean isInitSuc = doInitialize();
        if (!isInitSuc) {
            throw new UnexpectedException("OckShuffleJniWriter initialization failed");
        }
    }

    /**
     * make
     *
     * @param appId appId
     * @param shuffleId shuffleId
     * @param stageId stageId
     * @param stageAttemptNumber stageAttemptNumber
     * @param mapId mapId
     * @param taskAttemptId taskAttemptId
     * @param part part
     * @param capacity capacity
     * @param maxCapacity maxCapacity
     * @param minCapacity minCapacity
     * @param isCompress isCompress
     * @return splitterId
     */
    public long make(String appId, int shuffleId, int stageId, int stageAttemptNumber,
            int mapId, long taskAttemptId, PartitionInfo part, int capacity, int maxCapacity,
            int minCapacity, boolean isCompress) {
        return nativeMake(
                appId,
                shuffleId,
                stageId,
                stageAttemptNumber,
                mapId,
                taskAttemptId,
                part.getPartitionName(),
                part.getPartitionNum(),
                part.getInputTypes(),
                part.getNumCols(),
                capacity,
                maxCapacity,
                minCapacity,
                isCompress);
    }

    /**
     * Create ock shuffle native writer
     *
     * @param appId appId
     * @param shuffleId shuffleId
     * @param stageId stageId
     * @param stageAttemptNumber stageAttemptNumber
     * @param mapId mapId
     * @param taskAttemptId taskAttemptId
     * @param partitioningMethod partitioningMethod
     * @param numPartitions numPartitions
     * @param inputTpyes inputTpyes
     * @param numCols numCols
     * @param capacity capacity
     * @param maxCapacity maxCapacity
     * @param minCapacity minCapacity
     * @param isCompress isCompress
     * @return splitterId
     */
    public native long nativeMake(String appId, int shuffleId, int stageId, int stageAttemptNumber,
        int mapId, long taskAttemptId, String partitioningMethod, int numPartitions,
        String inputTpyes, int numCols, int capacity, int maxCapacity, int minCapacity,
        boolean isCompress);

    private boolean doInitialize() {
        return initialize();
    }

    private native boolean initialize();

    /**
     * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
     * split according to the first column as partition id. During splitting, the data in native
     * buffers will be write to disk when the buffers are full.
     *
     *  @param splitterId splitter instance id
     * @param nativeVectorBatch Addresses of nativeVectorBatch
     */
    public native void split(long splitterId, long nativeVectorBatch);

    /**
     * Write the data remained in the buffers hold by native splitter to each partition's temporary
     * file. And stop processing splitting
     *
     * @param splitterId splitter instance id
     * @return SplitResult
     */
    public native SplitResult stop(long splitterId);

    /**
     * Release resources associated with designated splitter instance.
     *
     * @param splitterId splitter instance id
     */
    public native void close(long splitterId);
}