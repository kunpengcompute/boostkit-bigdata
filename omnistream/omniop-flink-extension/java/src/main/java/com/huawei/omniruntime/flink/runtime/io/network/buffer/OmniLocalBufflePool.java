package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.GuardedBy;

/**
 * OmniLocalBufflePool
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class OmniLocalBufflePool extends OmniBufferPoolShadow {
    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    public OmniLocalBufflePool(long nativeBufferPoolAddress) {
        super(nativeBufferPoolAddress);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }
}
