package com.huawei.omniruntime.flink;

/**
 * TNELLibrary
 *
 * @since 2025-04-27
 */
public class TNELLibrary {
    private static native void initialize();

    /**
     * loadLibrary
     */
    public static void loadLibrary() {
        System.loadLibrary("tnel");
        System.out.println("Loading Task Native Execution Library");
        initialize();
    }
}
