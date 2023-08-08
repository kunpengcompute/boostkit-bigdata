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

    private static int getIntSysEnv(String envName, int defaultVal) {
        String val = System.getenv(envName);
        if (!isNumeric(val) && Integer.parseInt(val) < 0) {
            return defaultVal;
        }
        return Integer.parseInt(val);
    }

    private static String getIntStrSysEnv(String envName, String defaultVal) {
        String val = System.getenv(envName);
        if (!isNumeric(val) && Integer.parseInt(val) < 0) {
            return defaultVal;
        }
        return val;
    }

    public static boolean getNdpEnable() {
        String isEnable = System.getenv("NDP_PLUGIN_ENABLE");
        return isEnable != null && isEnable.equals("true");
    }

    public static int getPushDownTaskTotal(int taskTotal) {
        return getIntSysEnv("DEFAULT_PUSHDOWN_TASK", taskTotal);
    }

    public static String getNdpNumPartitionsStr(String numStr) {
        return getIntStrSysEnv("DEFAULT_NDP_NUM_PARTITIONS", numStr);
    }

    public static int getCountTaskTotal(int taskTotal) {
        return getIntSysEnv("COUNT_TASK_TOTAL", taskTotal);
    }

    public static String getCountMaxPartSize(String size) {
        return System.getenv("COUNT_MAX_PART_SIZE") != null ?
                System.getenv("COUNT_MAX_PART_SIZE") : size;
    }

    public static int getCountDistinctTaskTotal(int taskTotal) {
        return getIntSysEnv("COUNT_DISTINCT_TASK_TOTAL", taskTotal);
    }

    public static String getSMJMaxPartSize(String size) {
        return System.getenv("SMJ_MAX_PART_SIZE") != null ?
                System.getenv("SMJ_MAX_PART_SIZE") : size;
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

    public static String getSortRepartitionSizeStr(String sizeStr) {
        return getIntStrSysEnv("SORT_REPARTITION_SIZE", sizeStr);
    }

    public static String getCastDecimalPrecisionStr(String numStr) {
        return getIntStrSysEnv("CAST_DECIMAL_PRECISION", numStr);
    }

    public static String getNdpMaxPtFactorStr(String numStr) {
        return getIntStrSysEnv("NDP_MAX_PART_FACTOR", numStr);
    }

    public static String getCountAggMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("COUNT_AGG_MAX_FILE_BYTES") != null ?
                System.getenv("COUNT_AGG_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getAvgAggMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("AVG_AGG_MAX_FILE_BYTES") != null ?
                System.getenv("AVG_AGG_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getBhjMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("BHJ_MAX_FILE_BYTES") != null ?
                System.getenv("BHJ_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getGroupMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("GROUP_MAX_FILE_BYTES") != null ?
                System.getenv("GROUP_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getMixSqlBaseMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("MIX_SQL_BASE_MAX_FILE_BYTES") != null ?
                System.getenv("MIX_SQL_BASE_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getMixSqlAccurateMaxFilePtBytesStr(String bytesStr) {
        return System.getenv("MIX_SQL_ACCURATE_MAX_FILE_BYTES") != null ?
                System.getenv("MIX_SQL_ACCURATE_MAX_FILE_BYTES") : bytesStr;
    }

    public static String getAggShufflePartitionsStr(String byteNumStr) {
        return getIntStrSysEnv("AGG_SHUFFLE_PARTITIONS", byteNumStr);
    }

    public static String getShufflePartitionsStr(String byteNumStr) {
        return getIntStrSysEnv("SHUFFLE_PARTITIONS", byteNumStr);
    }

    public static String getSortShufflePartitionsStr(String byteNumStr) {
        return getIntStrSysEnv("SORT_SHUFFLE_PARTITIONS", byteNumStr);
    }
}
