package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import java.util.concurrent.CompletableFuture;

/**
 * OmniBufferPoolShadow
 *
 * @since 2025-04-27
 */
public class OmniBufferPoolShadow implements OmniBufferPool {
    /**
     * refers to an address pointing to a memory buffer pool in the underlying system
     */
    protected long nativeBufferPoolAddress;

    public OmniBufferPoolShadow(long nativeBufferPoolAddress) {
        this.nativeBufferPoolAddress = nativeBufferPoolAddress;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAvailable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isApproximatelyAvailable() {
        throw new UnsupportedOperationException();
    }
}
