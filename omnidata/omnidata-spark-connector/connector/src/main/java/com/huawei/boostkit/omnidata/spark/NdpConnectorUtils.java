package com.huawei.boostkit.omnidata.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class NdpConnectorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NdpConnectorUtils.class);

    public static Set<String> getIpAddress() {
        Set<String> ipSet = new HashSet<>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (!netInterface.isLoopback() && !netInterface.isVirtual() && netInterface.isUp()) {
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip instanceof Inet4Address) {
                            ipSet.add(ip.getHostAddress());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("getIpAddress exception:", e);
        }
        return ipSet;
    }

    public static String getNdpEnable() {
        return System.getenv("NDP_PLUGIN_ENABLE") == null ? "false" : System.getenv("NDP_PLUGIN_ENABLE");
    }

    public static int getPushDownTaskTotal(int taskTotal) {
        if (System.getenv("DEFAULT_PUSHDOWN_TASK") != null) {
            return Integer.parseInt(System.getenv("DEFAULT_PUSHDOWN_TASK"));
        } else {
            return taskTotal;
        }
    }

    public static String getNdpNumPartitionsStr(String numStr) {
        return System.getenv("DEFAULT_NDP_NUM_PARTITIONS") != null ?
                System.getenv("DEFAULT_NDP_NUM_PARTITIONS") : numStr;
    }


    public static int getCountTaskTotal(int taskTotal) {
        return System.getenv("COUNT_TASK_TOTAL") != null ?
                Integer.parseInt(System.getenv("COUNT_TASK_TOTAL")) : taskTotal;
    }

    public static String getCountMaxPartSize(String size) {
        return System.getenv("COUNT_MAX_PART_SIZE") != null ?
                System.getenv("COUNT_MAX_PART_SIZE") : size;
    }

    public static int getCountDistinctTaskTotal(int taskTotal) {
        return System.getenv("COUNT_DISTINCT_TASK_TOTAL") != null ?
                Integer.parseInt(System.getenv("COUNT_DISTINCT_TASK_TOTAL")) : taskTotal;
    }

    public static String getSMJMaxPartSize(String size) {
        return System.getenv("SMJ_MAX_PART_SIZE") != null ?
                System.getenv("SMJ_MAX_PART_SIZE") : size;
    }

    public static int getSMJNumPartitions(int numPartitions) {
        return System.getenv("SMJ_NUM_PARTITIONS") != null ?
                Integer.parseInt(System.getenv("SMJ_NUM_PARTITIONS")) : numPartitions;
    }

    public static int getOmniColumnarNumPartitions(int numPartitions) {
        return System.getenv("OMNI_COLUMNAR_PARTITIONS") != null ?
                Integer.parseInt(System.getenv("OMNI_COLUMNAR_PARTITIONS")) : numPartitions;
    }

    public static int getOmniColumnarTaskCount(int taskTotal) {
        return System.getenv("OMNI_COLUMNAR_TASK_TOTAL") != null ?
                Integer.parseInt(System.getenv("OMNI_COLUMNAR_TASK_TOTAL")) : taskTotal;
    }

    public static int getFilterPartitions(int numPartitions) {
        return System.getenv("FILTER_COLUMNAR_PARTITIONS") != null ?
                Integer.parseInt(System.getenv("FILTER_COLUMNAR_PARTITIONS")) : numPartitions;
    }

    public static int getFilterTaskCount(int taskTotal) {
        return System.getenv("FILTER_TASK_TOTAL") != null ?
                Integer.parseInt(System.getenv("FILTER_TASK_TOTAL")) : taskTotal;
    }

    public static String getSortRepartitionSizeStr(String sizeStr) {
        return System.getenv("SORT_REPARTITION_SIZE") != null ?
                System.getenv("SORT_REPARTITION_SIZE") : sizeStr;
    }

    public static String getCastDecimalPrecisionStr(String numStr) {
        return System.getenv("CAST_DECIMAL_PRECISION") != null ?
                System.getenv("CAST_DECIMAL_PRECISION") : numStr;
    }

    public static String getNdpMaxPtFactorStr(String numStr) {
        return System.getenv("NDP_MAX_PART_FACTOR") != null ?
                System.getenv("NDP_MAX_PART_FACTOR") : numStr;
    }

    public static String getCountAggMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("COUNT_AGG_MAX_FILE_BYTES") != null ?
                System.getenv("COUNT_AGG_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getAvgAggMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("AVG_AGG_MAX_FILE_BYTES") != null ?
                System.getenv("AVG_AGG_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getBhjMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("BHJ_MAX_FILE_BYTES") != null ?
                System.getenv("BHJ_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getGroupMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("GROUP_MAX_FILE_BYTES") != null ?
                System.getenv("GROUP_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getMixSqlBaseMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("MIX_SQL_BASE_MAX_FILE_BYTES") != null ?
                System.getenv("MIX_SQL_BASE_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getMixSqlAccurateMaxFilePtBytesStr(String BytesStr) {
        return System.getenv("MIX_SQL_ACCURATE_MAX_FILE_BYTES") != null ?
                System.getenv("MIX_SQL_ACCURATE_MAX_FILE_BYTES") : BytesStr;
    }

    public static String getAggShufflePartitionsStr(String BytesStr) {
        return System.getenv("AGG_SHUFFLE_PARTITIONS") != null ?
                System.getenv("AGG_SHUFFLE_PARTITIONS") : BytesStr;
    }

    public static String getShufflePartitionsStr(String BytesStr) {
        return System.getenv("SHUFFLE_PARTITIONS") != null ?
                System.getenv("SHUFFLE_PARTITIONS") : BytesStr;
    }

    public static String getSortShufflePartitionsStr(String BytesStr) {
        return System.getenv("SORT_SHUFFLE_PARTITIONS") != null ?
                System.getenv("SORT_SHUFFLE_PARTITIONS") : BytesStr;
    }

}
