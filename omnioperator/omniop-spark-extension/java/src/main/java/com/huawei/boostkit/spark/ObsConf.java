/*
 * Copyright (C) 2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark;

import com.huawei.boostkit.spark.ColumnarPluginConfig;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.model.ISecurityKey;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObsConf {
    private static final Logger LOG = LoggerFactory.getLogger(ObsConf.class);

    private static String endpoint;
    private static String accessKey = "";
    private static String secretKey = "";
    private static String token = "";
    private static IObsCredentialsProvider securityProvider;
    private static boolean syncToGetToken = false;
    private static byte[] lock = new byte[0];

    private ObsConf() {
    }

    private static void init() {
        Configuration conf = new Configuration();
        String endpointConf = "fs.obs.endpoint";
        String accessKeyConf = "fs.obs.access.key";
        String secretKeyConf = "fs.obs.secret.key";
        String providerConf = "fs.obs.security.provider";
        endpoint = conf.get(endpointConf, "");
        if ("".equals(endpoint)) {
            LOG.warn("Key parameter {} is missing in the configuration file.", endpointConf);
            return;
        }
        accessKey = conf.get(accessKeyConf, "");
        secretKey = conf.get(secretKeyConf, "");
        if ("".equals(accessKey) && "".equals(secretKey)) {
            if ("".equals(conf.get(providerConf, ""))) {
                LOG.error("Key parameters such as {}, {}, or {} are missing or the parameter value is incorrect.",
                        accessKeyConf, secretKeyConf, providerConf);
            } else {
                getSecurityKey(conf, providerConf);
            }
        }
        syncToGetToken = ColumnarPluginConfig.getConf().enableSyncGetObsToken();
    }

    private static void getSecurityKey(Configuration conf, String providerConf) {
        try {
            Class<?> securityProviderClass = conf.getClass(providerConf, null);

            if (securityProviderClass == null) {
                LOG.error("Failed to get securityProviderClass {}.", conf.get(providerConf, ""));
                return;
            }

            securityProvider = (IObsCredentialsProvider) securityProviderClass.getDeclaredConstructor().newInstance();
            updateSecurityKey();
            if (!syncToGetToken) {
                timerGetSecurityKey();
            }
        } catch (Exception e) {
            LOG.error("get obs ak/sk/token failed.");
        }
    }

    private static void updateSecurityKey() {
        ISecurityKey iSecurityKey = securityProvider.getSecurityKey();
        synchronized (lock) {
            accessKey = iSecurityKey.getAccessKey();
            secretKey = iSecurityKey.getSecretKey();
            token = iSecurityKey.getSecurityToken();
        }
    }

    private static void timerGetSecurityKey() {
        Thread updateKeyThread = new Thread(new MyRunnable());
        updateKeyThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Failed to get securityKey: {}, {}", t.getName(), e.getMessage());
            }
        });
        updateKeyThread.start();
    }

    public static String getEndpoint() {
        if (endpoint == null) {
            init();
        }
        return endpoint;
    }

    public static String getAk() {
        if (syncToGetToken) {
            updateSecurityKey();
        }
        return accessKey;
    }

    public static String getSk() {
        return secretKey;
    }

    public static String getToken() {
        return token;
    }

    public static byte[] getLock() {
        return lock;
    }

    private static class MyRunnable implements Runnable {
        @Override
        public void run() {
            long sleepTime = ColumnarPluginConfig.getConf().timeGetObsToken();
            while (true) {
                try {
                    updateSecurityKey();
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
