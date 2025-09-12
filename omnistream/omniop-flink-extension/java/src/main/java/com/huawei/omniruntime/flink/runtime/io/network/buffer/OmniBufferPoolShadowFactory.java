package com.huawei.omniruntime.flink.runtime.io.network.buffer;

/**
 * OmniBufferPoolShadowFactory
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class OmniBufferPoolShadowFactory {
    /**
     * createBufferPool
     *
     * @param nativeBufferPoolAddress nativeBufferPoolAddress
     * @return OmniBufferPool
     */
    public static OmniBufferPool createBufferPool(long nativeBufferPoolAddress) {
        return new OmniBufferPoolShadow(nativeBufferPoolAddress);
    }
}
