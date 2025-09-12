package com.huawei.omniruntime.flink.runtime.execution;

import org.apache.flink.runtime.execution.Environment;

/**
 * OmniEnvironment
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public interface OmniEnvironment extends Environment {
    /**
     * getNativeEnvironmentRef
     *
     * @return long
     */
    long getNativeEnvironmentRef();
}
