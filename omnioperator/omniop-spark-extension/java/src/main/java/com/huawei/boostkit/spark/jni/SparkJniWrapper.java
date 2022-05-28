/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.spark.jni;

import com.huawei.boostkit.spark.vectorized.PartitionInfo;
import com.huawei.boostkit.spark.vectorized.SplitResult;

public class SparkJniWrapper {

    public SparkJniWrapper() {
        NativeLoader.getInstance();
    }

    public long make(PartitionInfo part,
                     int bufferSize,
                     String codec,
                     String dataFile,
                     int subDirsPerLocalDir,
                     String localDirs,
                     long shuffleCompressBlockSize,
                     int shuffleSpillBatchRowNum,
                     long shuffleSpillMemoryThreshold) {
        return nativeMake(
                part.getPartitionName(),
                part.getPartitionNum(),
                part.getInputTypes(),
                part.getNumCols(),
                bufferSize,
                codec,
                dataFile,
                subDirsPerLocalDir,
                localDirs,
                shuffleCompressBlockSize,
                shuffleSpillBatchRowNum,
                shuffleSpillMemoryThreshold);
    }

    public native long nativeMake(
            String shortName,
            int numPartitions,
            String inputTypes,
            int numCols,
            int bufferSize,
            String codec,
            String dataFile,
            int subDirsPerLocalDir,
            String localDirs,
            long shuffleCompressBlockSize,
            int shuffleSpillBatchRowNum,
            long shuffleSpillMemoryThreshold
    );

    /**
     * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
     * split according to the first column as partition id. During splitting, the data in native
     * buffers will be write to disk when the buffers are full.
     *
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
