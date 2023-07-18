/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.ock.spark.jni;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * NativeLoader
 *
 * @since 2022-6-10
 */
public enum NativeLoader {
    INSTANCE;

    private final String libraryName = "ock_columnar_shuffle";
    private final Logger LOG = LoggerFactory.getLogger(NativeLoader.class);
    private final int bufferSize = 1024;

    NativeLoader() {
        String nativeLibraryPath = File.separator + System.mapLibraryName(libraryName);
        File tempFile = null;
        try (InputStream in = NativeLoader.class.getResourceAsStream(nativeLibraryPath);
            FileOutputStream fos = new FileOutputStream(tempFile =
            File.createTempFile(libraryName, ".so"))) {
            int num;
            byte[] buf = new byte[bufferSize];
            while ((num = in.read(buf)) != -1) {
                fos.write(buf, 0, num);
            }

            System.load(tempFile.getCanonicalPath());
            tempFile.deleteOnExit();
        } catch (IOException e) {
            LOG.warn("fail to load library from Jar!errmsg:{}", e.getMessage());
            System.loadLibrary(libraryName);
        }
    }

    public static NativeLoader getInstance() {
        return INSTANCE;
    }
}