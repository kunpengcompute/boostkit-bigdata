/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
package com.huawei.boostkit.spark.jni;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import nova.hetu.omniruntime.utils.NativeLog;

/**
 * @since 2021.08
 */

public class NativeLoader {

    private static volatile NativeLoader INSTANCE;
    private static final String LIBRARY_NAME = "spark_columnar_plugin";
    private static final Logger LOG = LoggerFactory.getLogger(NativeLoader.class);
    private static final int BUFFER_SIZE = 1024;

    public static NativeLoader getInstance() {
        if (INSTANCE == null){
            synchronized (NativeLoader.class) {
                if (INSTANCE == null){
                    INSTANCE = new NativeLoader();
                }
            }
        }
        return INSTANCE;
    }

    private NativeLoader() {
        try {
            String nativeLibraryPath = File.separator +
                    System.mapLibraryName(LIBRARY_NAME);
            InputStream in = NativeLoader.class.getResourceAsStream(nativeLibraryPath);
            File tempFile = File.createTempFile(LIBRARY_NAME, ".so");
            FileOutputStream fos = new FileOutputStream(tempFile);
            int i;
            byte[] buf = new byte[BUFFER_SIZE];
            while ((i = in.read(buf)) != -1) {
                fos.write(buf, 0, i);
            }
            in.close();
            fos.close();
            System.load(tempFile.getAbsolutePath());
            NativeLog.getInstance();
            tempFile.deleteOnExit();
        }catch (IOException e) {
            LOG.warn("fail to load library from Jar!errmsg:{}",e.getMessage());
            System.loadLibrary(LIBRARY_NAME);
        }
    }
}
