/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package com.huawei.boostkit.omnidata.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class NdpConnectorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NdpConnectorUtils.class);
    public static Set<String> getIpAddress() {
        Set<String> ipSet = new HashSet<>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        ipSet.add(ip.getHostAddress());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("getIpAddress exception:", e);
        }
        return ipSet;
    }

    public static boolean isNumeric(String str) {
        return str != null && str.chars().allMatch(Character::isDigit);
    }

    public static boolean getNdpEnable() {
        String isEnable = System.getenv("NDP_PLUGIN_ENABLE");
        return isEnable != null && isEnable.equals("true");
    }

    private static int getIntSysEnv(String envName, int defaultVal) {
        String val = System.getenv(envName);
        if (!isNumeric(val) && Integer.parseInt(val) < 0) {
            return defaultVal;
        }
        return Integer.parseInt(val);
    }

    public static int getSMJNumPartitions(int numPartitions) {
        return getIntSysEnv("SMJ_NUM_PARTITIONS", numPartitions);
    }

    public static int getOmniColumnarNumPartitions(int numPartitions) {
        return getIntSysEnv("OMNI_COLUMNAR_PARTITIONS", numPartitions);
    }

    public static int getOmniColumnarTaskCount(int taskTotal) {
        return getIntSysEnv("OMNI_COLUMNAR_TASK_TOTAL", taskTotal);
    }

    public static int getFilterPartitions(int numPartitions) {
        return getIntSysEnv("FILTER_COLUMNAR_PARTITIONS", numPartitions);
    }

    public static int getFilterTaskCount(int taskTotal) {
        return getIntSysEnv("FILTER_TASK_TOTAL", taskTotal);
    }

}
