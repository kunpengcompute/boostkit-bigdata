/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.util;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SysInfoNodeExporter extends SysInfoLinux {
    private static final Logger LOG =
            LoggerFactory.getLogger(SysInfoNodeExporter.class);

    private String nodeExporterUrl;

    private static final int STATUS_CODE_OK = 200;

    private List<String> nodeExporterResponse = new ArrayList<>();

    private static final Pattern NODE_DISK_IO_TIME_SECONDS_TOTAL =
            Pattern.compile("^node_disk_io_time_seconds_total" +
                    "\\{device=\"(\\w+)\"\\}\\s+" +
                    "(\\d+(\\.\\d+)?(e[+-]?\\d+)?)$");

    private static final Pattern NODE_FILESYSTEM_AVAIL_BYTES =
            Pattern.compile("^node_filesystem_avail_bytes" +
                    "\\{device=\"([/\\w]+)\",fstype=.*,mountpoint=\"([/\\w]+)\"\\}\\s+" +
                    "(\\d+(\\.\\d+)?(e[+-]?\\d+)?)$");

    private static final Pattern NODE_NETWORK =
            Pattern.compile("^node_network_(\\w+)" +
                    "\\{device=\"(\\w+)\"\\}\\s+" +
                    "(\\d+(\\.\\d+)?(e[+-]?\\d+)?)$");

    private static final String NODE_NETWORK_RECEIVE_BYTES_TOTAL = "receive_bytes_total";
    private static final String NODE_NETWORK_TRANSMIT_BYTES_TOTAL = "transmit_bytes_total";
    private static final String NODE_NETWORK_SPEED_BYTES = "speed_bytes";

    private static final Pattern NODE_CPU_SECONDS_TOTAL =
            Pattern.compile("^node_cpu_seconds_total" +
                    "\\{cpu=\"(\\w+)\",mode=\"(\\w+)\"\\}\\s+" +
                    "(\\d+(\\.\\d+)?(e[+-]?\\d+)?)$");

    private static final String NODE_CPU_USER_TIME = "user";
    private static final String NODE_CPU_NICE_TIME = "nice";
    private static final String NODE_CPU_SYSTEM_TIME = "system";

    private static final Pattern NODE_MEMORY =
            Pattern.compile("^node_memory_(\\w+)_bytes" +
                    "\\s+" +
                    "(\\d+(\\.\\d+)?(e[+-]?\\d+)?)$");

    private static final String NODE_MEMORY_BUFFER = "Buffers";
    private static final String NODE_MEMORY_CACHE = "Cached";
    private static final String NODE_MEMORY_MEMFREE = "MemFree";
    private static final String NODE_MEMORY_MEMTOTAL = "MemTotal";


    public SysInfoNodeExporter(String nodeExporterUrl) {
        this.nodeExporterUrl = nodeExporterUrl;
    }

    /**
     * request node exporter http interface and save data to nodeExporterResponse
     */
    public void requestNodeExporter() {
        nodeExporterResponse.clear();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(nodeExporterUrl);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            nodeExporterResponse.add(line);
                        }
                    }
                }
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != STATUS_CODE_OK) {
                    LOG.error("response code: " + statusCode + " for " + nodeExporterResponse);
                }
            }
        } catch (IOException e) {
            LOG.error("monitor failed for ", e);
        }
    }

    /**
     * Read nodeExporterResponse, parse and calculate amount
     * of bytes read and written from/to disks.
     */
    protected void readProcDisksInfoFile(String[] paths) {
        Matcher mat;
        numDisks = 0;
        diskTotalIoTimeMap.clear();
        isDataDiskMap.clear();
        long totalIoPerDisk = 0;
        for (String str : nodeExporterResponse) {
            mat = NODE_DISK_IO_TIME_SECONDS_TOTAL.matcher(str);
            if (mat.find()) {
                String diskName = mat.group(1);
                assert diskName != null;
                // ignore loop or ram partitions
                if (diskName.contains("loop") || diskName.contains("ram")) {
                    continue;
                }
                diskTotalIoTimeMap.put(diskName, Double.parseDouble(mat.group(2)) * 1000);
                continue;
            }

            mat = NODE_FILESYSTEM_AVAIL_BYTES.matcher(str);
            if (mat.find()) {
                String device = mat.group(1);
                assert device != null;
                // ignore loop or ram or tmpfs partitions
                if (device.contains("loop") || device.contains("ram") || device.contains("tmpfs")) {
                    continue;
                }

                // get true name from eg: /dev/sda1  => sda
                String[] deviceName = device.split("/");
                String diskName = deviceName[deviceName.length - 1].replaceAll("\\d+", "");

                String mountPoint = mat.group(2);
                boolean isDataDisk = (mountPoint != null) && (!mountPoint.equals("/"))
                        && (!mountPoint.startsWith("/boot"));
                isDataDiskMap.put(diskName, isDataDisk);
            }
        }

        for (Map.Entry<String, Double> diskTotalIoTime : diskTotalIoTimeMap.entrySet()) {
            if (isDataDiskMap.getOrDefault(diskTotalIoTime.getKey(), false)) {
                totalIoPerDisk += diskTotalIoTime.getValue();
                numDisks++;
            }
        }

        diskIoTimeTracker.updateElapsedJiffies(
                BigInteger.valueOf(totalIoPerDisk),
                getCurrentTime());
    }

    /**
     * Read nodeExporterResponse, parse and calculate amount
     * of bytes read and written through the network by main enp.
     */
    protected void readProcMainNetInfoFile(String computeType) {
        Map<String, Long[]> netBytesReadAndWritten = new HashMap<>();

        Matcher mat;
        for (String str : nodeExporterResponse) {
            mat = NODE_NETWORK.matcher(str);
            if (mat.find()) {
                // ignore loopback interfaces
                if (mat.group(2).equals("lo")) {
                    continue;
                }
                String type = mat.group(1);
                String name = mat.group(2);
                Long[] bytesReadAndWritten = netBytesReadAndWritten.computeIfAbsent(name, s -> new Long[2]);
                switch (type) {
                    case NODE_NETWORK_RECEIVE_BYTES_TOTAL:
                        long read = (long) Double.parseDouble(mat.group(3));
                        bytesReadAndWritten[0] = read;
                        break;
                    case NODE_NETWORK_TRANSMIT_BYTES_TOTAL:
                        long write = (long) Double.parseDouble(mat.group(3));
                        bytesReadAndWritten[1] = write;
                        break;
                    case NODE_NETWORK_SPEED_BYTES:
                        long speedMBSecond = (long) Double.parseDouble(mat.group(3)) / 1024 / 1024;
                        networkSpeedLimitMap.put(name, speedMBSecond);
                        break;
                    default:
                        break;
                }
            }
        }
        for (Map.Entry<String, Long[]> entry : netBytesReadAndWritten.entrySet()) {
            String name = entry.getKey();
            long read = entry.getValue()[0];
            long write = entry.getValue()[1];
            long speedLimit = getNetworkSpeedLimit(name);
            if (speedLimit <= 0) {
                continue;
            }
            netIoSendTimeTrackerMap.get(name).updateElapsedJiffies(
                    BigInteger.valueOf(computeNetworkTimeMs(
                            write,
                            speedLimit)),
                    getCurrentTime());
            netIoReceiveTimeTrackerMap.get(name).updateElapsedJiffies(
                    BigInteger.valueOf(computeNetworkTimeMs(
                            read,
                            speedLimit)),
                    getCurrentTime());
        }
    }

    /**
     * do this in readProcMainNetInfoFile, overwrite parent method
     *
     * @param networkInterfaceName String
     */
    protected void readNetworkSpeedLimit(String networkInterfaceName) {
    }

    /**
     * Read nodeExporterResponse, parse and calculate cumulative CPU.
     */
    protected void readProcStatFile() {
        long runTime = 0;
        long totalTime = 0;
        numProcessors = 0;

        Matcher mat;
        for (String str : nodeExporterResponse) {
            mat = NODE_CPU_SECONDS_TOTAL.matcher(str);
            if (mat.find()) {
                String mode = mat.group(2);
                long time = (long) (Double.parseDouble(mat.group(3)) * 1000);
                switch (mode) {
                    case NODE_CPU_USER_TIME:
                        numProcessors++;
                    case NODE_CPU_NICE_TIME:
                    case NODE_CPU_SYSTEM_TIME:
                        runTime += time;
                    default:
                        totalTime += time;
                        break;
                }
            }
        }
        cpuTimeTracker.updateElapsedJiffies(
                BigInteger.valueOf(runTime),
                totalTime);
    }

    /**
     * getCpuUsagePercentage
     *
     * @return CpuUsagePercentage
     */
    public float getCpuUsagePercentage() {
        readProcStatFile();
        return cpuTimeTracker.getUsagePercent();
    }

    /**
     * getPhyMemUsagePercentage
     *
     * @return PhyMemUsagePercentage
     */
    public float getPhyMemUsagePercentage() {
        long availablePhysicalMemorySize = 0;
        long physicalMemorySize = 0;

        Matcher mat;
        for (String str : nodeExporterResponse) {
            mat = NODE_MEMORY.matcher(str);
            if (mat.find()) {
                String mode = mat.group(1);
                long bytes = (long) (Double.parseDouble(mat.group(2)));
                switch (mode) {
                    case NODE_MEMORY_BUFFER:
                    case NODE_MEMORY_CACHE:
                    case NODE_MEMORY_MEMFREE:
                        availablePhysicalMemorySize += bytes;
                        break;
                    case NODE_MEMORY_MEMTOTAL:
                        physicalMemorySize = bytes;
                        break;
                    default:
                        break;
                }
            }
        }
        return (1.0f - (float) availablePhysicalMemorySize / (float) physicalMemorySize) * 100;
    }
}