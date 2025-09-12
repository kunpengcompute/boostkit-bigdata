/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.exception;

/**
 * GeneralRuntimeException is a custom runtime exception class that extends RuntimeException.
 * It provides constructors to create exceptions with a message, a cause, or both.
 * This class is used to handle general runtime errors in the application.
 *
 * @since 2025-04-29
 */
public class GeneralRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public GeneralRuntimeException(String message) {
        super(message);
    }

    public GeneralRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneralRuntimeException(Throwable cause) {
        super(cause);
    }
}
