/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.bdsolution.loadsmetric.util;

import com.huawei.bdsolution.loadsmetric.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.huawei.bdsolution.loadsmetric.util.DF.DF_INTERVAL_DEFAULT;

/**
 * Plugin to calculate resource information on Linux systems.
 */
public class SysInfoLinux {
    private static final Logger LOG =
            LoggerFactory.getLogger(SysInfoLinux.class);

    /**
     * proc's meminfo virtual file has keys-values in the format
     * "key:[ \t]*value[ \t]kB".
     */
    private static final String PROCFS_MEMFILE = "/proc/meminfo";
    private static final Pattern PROCFS_MEMFILE_FORMAT =
            Pattern.compile("^([a-zA-Z_()]*):[ \t]*([0-9]*)[ \t]*(kB)?");

    // We need the values for the following keys in meminfo
    private static final String MEMTOTAL_STRING = "MemTotal";
    private static final String SWAPTOTAL_STRING = "SwapTotal";
    private static final String MEMFREE_STRING = "MemFree";
    private static final String SWAPFREE_STRING = "SwapFree";
    private static final String INACTIVE_STRING = "Inactive";
    private static final String INACTIVEFILE_STRING = "Inactive(file)";
    private static final String HARDWARECORRUPTED_STRING = "HardwareCorrupted";
    private static final String HUGEPAGESTOTAL_STRING = "HugePages_Total";
    private static final String HUGEPAGESIZE_STRING = "Hugepagesize";

    /**
     * Patterns for parsing /proc/cpuinfo.
     */
    private static final String PROCFS_CPUINFO = "/proc/cpuinfo";
    private static final Pattern PROCESSOR_FORMAT =
            Pattern.compile("^processor[ \t]:[ \t]*([0-9]*)");
    private static final Pattern FREQUENCY_FORMAT =
            Pattern.compile("^cpu MHz[ \t]*:[ \t]*([0-9.]*)");
    private static final Pattern PHYSICAL_ID_FORMAT =
            Pattern.compile("^physical id[ \t]*:[ \t]*([0-9]*)");
    private static final Pattern CORE_ID_FORMAT =
            Pattern.compile("^core id[ \t]*:[ \t]*([0-9]*)");

    /**
     * Pattern for parsing /proc/stat.
     */
    private static final String PROCFS_STAT = "/proc/stat";
    private static final Pattern CPU_TIME_FORMAT =
            Pattern.compile("^cpu[ \t]*([0-9]*)" +
                    "[ \t]*([0-9]*)[ \t]*([0-9]*)[ \t].*");
    protected TimeTracker cpuTimeTracker;

    /**
     * Pattern for parsing /proc/net/dev.
     */
    private static final String PROCFS_NETFILE = "/proc/net/dev";
    private static final Pattern PROCFS_NETFILE_FORMAT =
            Pattern .compile("^[ \t]*([a-zA-Z]+[0-9a-zA-Z]*):" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+).*");

    /**
     * Pattern for parsing /proc/diskstats.
     */
    private static final String PROCFS_DISKSFILE = "/proc/diskstats";
    private static final Pattern PROCFS_DISKSFILE_FORMAT =
            Pattern.compile("^[ \t]*([0-9]+)[ \t]*([0-9 ]+)" +
                    "(?!([a-zA-Z]+[0-9]+))([a-zA-Z]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)" +
                    "[ \t]*([0-9]+)[ \t]*([0-9]+)[ \t]*([0-9]+)");
    protected TimeTracker diskIoTimeTracker;

    /**
     * Pattern for parsing /sys/block/partition_name/queue/hw_sector_size.
     */
    private static final Pattern PROCFS_DISKSECTORFILE_FORMAT =
            Pattern.compile("^([0-9]+)");

    /** How net usage compute */
    /**
     * net speed compute type,
     */
    public static final String NET_COMPUTE_TYPE = "net_compute_type";
    public static final String NM_RESOURCE_MON_NET_COMPUTE_TYPE_SEND = "send";
    public static final String NM_RESOURCE_MON_NET_COMPUTE_TYPE_RECEIVE = "receive";
    public static final String NM_RESOURCE_MON_NET_COMPUTE_TYPE_AVERAGE = "average";
    public static final String NM_RESOURCE_MON_NET_COMPUTE_TYPE_MAX = "max";

    private String procfsMemFile;
    private String procfsCpuFile;
    private String procfsStatFile;
    private String procfsNetFile;
    private String procfsDisksFile;
    private long jiffyLengthInMillis;

    // cache mount path and disk mapping relation
    private String[] lastPaths;
    private List<String> mountList;

    private long ramSize = 0;
    private long swapSize = 0;
    private long ramSizeFree = 0;  // free ram space on the machine (kB)
    private long swapSizeFree = 0; // free swap space on the machine (kB)
    private long inactiveSize = 0; // inactive memory (kB)
    private long inactiveFileSize = -1; // inactive cache memory, -1 if not there
    private long hardwareCorruptSize = 0; // RAM corrupt and not available
    private long hugePagesTotal = 0; // # of hugepages reserved
    private long hugePageSize = 0; // # size of each hugepage


    /* number of logical processors on the system. */
    protected int numProcessors = 0;
    /* number of physical cores on the system. */
    private int numCores = 0;
    /* number of disks on the system. */
    protected int numDisks = 0;
    private long cpuFrequency = 0L; // CPU frequency on the system (kHz)
    private long numNetBytesRead = 0L; // aggregated bytes read from network
    private long numNetBytesWritten = 0L; // aggregated bytes written to network
    private long numDisksBytesRead = 0L; // aggregated bytes read from disks
    private long numDisksBytesWritten = 0L; // aggregated bytes written to disks

    private boolean readMemInfoFile = false;
    private boolean readCpuInfoFile = false;

    /* map for every disk its sector size */
    private HashMap<String, Integer> perDiskSectorSize = null;

    public static final long PAGE_SIZE = getConf("PAGESIZE");
    public static final long JIFFY_LENGTH_IN_MILLIS =
            Math.max(Math.round(1000D / getConf("CLK_TCK")), -1);

    protected Map<String, Long> networkSpeedLimitMap = new ConcurrentHashMap<>();
    private static final Pattern NETWORK_SPEED_LIMIT =
            Pattern.compile("^([ \t]*Speed):[ \t]*([0-9]+)(Mb/s)?");
    protected Map<String, TimeTracker> netIoSendTimeTrackerMap = new HashMap<>();
    protected Map<String, TimeTracker> netIoReceiveTimeTrackerMap = new HashMap<>();

    private static final int MINUTE_SECONDS = 60;
    private static final int HOUR_SECONDS = 3600;
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final DecimalFormat usageFormatter = new DecimalFormat("#.##");

    protected final Map<String, Boolean> isDataDiskMap = new HashMap<>();
    protected final Map<String, Double> diskTotalIoTimeMap = new HashMap<>();

    private static long getConf(String attr) {
        if(Shell.LINUX) {
            try {
                ShellCommandExecutor shellExecutorClk = new ShellCommandExecutor(
                        new String[] {"getconf", attr });
                shellExecutorClk.execute();
                return Long.parseLong(shellExecutorClk.getOutput().replace("\n", ""));
            } catch (IOException|NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Get current time.
     * @return Unix time stamp in millisecond
     */
    long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public SysInfoLinux() {
        this(PROCFS_MEMFILE, PROCFS_CPUINFO, PROCFS_STAT,
                PROCFS_NETFILE, PROCFS_DISKSFILE, JIFFY_LENGTH_IN_MILLIS);
    }

    /**
     * Constructor which allows assigning the /proc/ directories. This will be
     * used only in unit tests.
     * @param procfsMemFile fake file for /proc/meminfo
     * @param procfsCpuFile fake file for /proc/cpuinfo
     * @param procfsStatFile fake file for /proc/stat
     * @param procfsNetFile fake file for /proc/net/dev
     * @param procfsDisksFile fake file for /proc/diskstats
     * @param jiffyLengthInMillis fake jiffy length value
     */
    public SysInfoLinux(String procfsMemFile,
                        String procfsCpuFile,
                        String procfsStatFile,
                        String procfsNetFile,
                        String procfsDisksFile,
                        long jiffyLengthInMillis) {
        this.procfsMemFile = procfsMemFile;
        this.procfsCpuFile = procfsCpuFile;
        this.procfsStatFile = procfsStatFile;
        this.procfsNetFile = procfsNetFile;
        this.procfsDisksFile = procfsDisksFile;
        this.jiffyLengthInMillis = jiffyLengthInMillis;
        this.cpuTimeTracker = new TimeTracker("cpu", jiffyLengthInMillis);
        this.perDiskSectorSize = new HashMap<String, Integer>();
        this.diskIoTimeTracker = new TimeTracker("disk", jiffyLengthInMillis);
    }

    /**
     * Read /proc/meminfo, parse and compute memory information only once.
     */
    private void readProcMemInfoFile() {
        readProcMemInfoFile(false);
    }

    /**
     *
     * Wrapper for Long.parseLong() that returns zero if the value is
     * invalid. Under some circumstances, swapFree in /proc/meminfo can
     * go negative, reported as a very large decimal value.
     */
    private long safeParseLong(String strVal) {
        long parsedVal;
        try {
            parsedVal = Long.parseLong(strVal);
        } catch (NumberFormatException nfe) {
            parsedVal = 0;
        }
        return parsedVal;
    }
    /**
     * Read /proc/meminfo, parse and compute memory information.
     * @param readAgain if false, read only on the first time
     */
    private void readProcMemInfoFile(boolean readAgain) {

        if (readMemInfoFile && !readAgain) {
            return;
        }

        // Read "/proc/memInfo" file
        BufferedReader in;
        InputStreamReader fReader;
        try {
            fReader = new InputStreamReader(
                    Files.newInputStream(Paths.get(procfsMemFile)),
                    Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (IOException f) {
            // shouldn't happen....
            LOG.warn("Couldn't read " + procfsMemFile
                    + "; can't determine memory settings");
            return;
        }

        Matcher mat;

        try {
            String str = in.readLine();
            while (str != null) {
                mat = PROCFS_MEMFILE_FORMAT.matcher(str);
                if (mat.find()) {
                    if (mat.group(1).equals(MEMTOTAL_STRING)) {
                        ramSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(SWAPTOTAL_STRING)) {
                        swapSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(MEMFREE_STRING)) {
                        ramSizeFree = safeParseLong(mat.group(2));
                    } else if (mat.group(1).equals(SWAPFREE_STRING)) {
                        swapSizeFree = safeParseLong(mat.group(2));
                    } else if (mat.group(1).equals(INACTIVE_STRING)) {
                        inactiveSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(INACTIVEFILE_STRING)) {
                        inactiveFileSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(HARDWARECORRUPTED_STRING)) {
                        hardwareCorruptSize = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(HUGEPAGESTOTAL_STRING)) {
                        hugePagesTotal = Long.parseLong(mat.group(2));
                    } else if (mat.group(1).equals(HUGEPAGESIZE_STRING)) {
                        hugePageSize = Long.parseLong(mat.group(2));
                    }
                }
                str = in.readLine();
            }
        } catch (IOException io) {
            LOG.warn("Error reading the stream " + io);
        } finally {
            // Close the streams
            try {
                fReader.close();
                try {
                    in.close();
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + in);
                }
            } catch (IOException i) {
                LOG.warn("Error closing the stream " + fReader);
            }
        }

        readMemInfoFile = true;
    }

    /**
     * Read /proc/cpuinfo, parse and calculate CPU information.
     */
    private void readProcCpuInfoFile() {
        // This directory needs to be read only once
        if (readCpuInfoFile) {
            return;
        }
        HashSet<String> coreIdSet = new HashSet<>();
        // Read "/proc/cpuinfo" file
        BufferedReader in;
        InputStreamReader fReader;
        try {
            fReader =
                    new InputStreamReader(Files.newInputStream(Paths.get(procfsCpuFile)),
                            Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (IOException f) {
            // shouldn't happen....
            LOG.warn("Couldn't read " + procfsCpuFile + "; can't determine cpu info");
            return;
        }
        Matcher mat;
        try {
            numProcessors = 0;
            numCores = 1;
            String currentPhysicalId = "";
            String str = in.readLine();
            while (str != null) {
                mat = PROCESSOR_FORMAT.matcher(str);
                if (mat.find()) {
                    numProcessors++;
                }
                mat = FREQUENCY_FORMAT.matcher(str);
                if (mat.find()) {
                    cpuFrequency = (long)(Double.parseDouble(mat.group(1)) * 1000); // kHz
                }
                mat = PHYSICAL_ID_FORMAT.matcher(str);
                if (mat.find()) {
                    currentPhysicalId = str;
                }
                mat = CORE_ID_FORMAT.matcher(str);
                if (mat.find()) {
                    coreIdSet.add(currentPhysicalId + " " + str);
                    numCores = coreIdSet.size();
                }
                str = in.readLine();
            }
        } catch (IOException io) {
            LOG.warn("Error reading the stream " + io);
        } finally {
            // Close the streams
            try {
                fReader.close();
                try {
                    in.close();
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + in);
                }
            } catch (IOException i) {
                LOG.warn("Error closing the stream " + fReader);
            }
        }
        readCpuInfoFile = true;
    }

    /**
     * Read /proc/stat file, parse and calculate cumulative CPU.
     */
    protected void readProcStatFile() {
        // Read "/proc/stat" file
        BufferedReader in;
        InputStreamReader fReader;
        try {
            fReader = new InputStreamReader(
                    Files.newInputStream(Paths.get(procfsStatFile)),
                    Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (IOException f) {
            // shouldn't happen....
            return;
        }

        Matcher mat;
        try {
            String str = in.readLine();
            while (str != null) {
                mat = CPU_TIME_FORMAT.matcher(str);
                if (mat.find()) {
                    long uTime = Long.parseLong(mat.group(1));
                    long nTime = Long.parseLong(mat.group(2));
                    long sTime = Long.parseLong(mat.group(3));
                    cpuTimeTracker.updateElapsedJiffies(
                            BigInteger.valueOf(uTime + nTime + sTime).multiply(BigInteger.valueOf(JIFFY_LENGTH_IN_MILLIS)),
                            getCurrentTime());
                    break;
                }
                str = in.readLine();
            }
        } catch (IOException io) {
            LOG.warn("Error reading the stream " + io);
        } finally {
            // Close the streams
            try {
                fReader.close();
                try {
                    in.close();
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + in);
                }
            } catch (IOException i) {
                LOG.warn("Error closing the stream " + fReader);
            }
        }
    }

    /**
     * Read /proc/net/dev file, parse and calculate amount
     * of bytes read and written through the network.
     */
    private void readProcNetInfoFile() {

        numNetBytesRead = 0L;
        numNetBytesWritten = 0L;

        // Read "/proc/net/dev" file
        BufferedReader in;
        InputStreamReader fReader;
        try {
            fReader = new InputStreamReader(
                    Files.newInputStream(Paths.get(procfsNetFile)),
                    Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (IOException f) {
            return;
        }

        Matcher mat;
        try {
            String str = in.readLine();
            while (str != null) {
                mat = PROCFS_NETFILE_FORMAT.matcher(str);
                if (mat.find()) {
                    assert mat.groupCount() >= 16;

                    // ignore loopback interfaces
                    if (mat.group(1).equals("lo")) {
                        str = in.readLine();
                        continue;
                    }
                    numNetBytesRead += Long.parseLong(mat.group(2));
                    numNetBytesWritten += Long.parseLong(mat.group(10));
                }
                str = in.readLine();
            }
        } catch (IOException io) {
            LOG.warn("Error reading the stream " + io);
        } finally {
            // Close the streams
            try {
                fReader.close();
                try {
                    in.close();
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + in);
                }
            } catch (IOException i) {
                LOG.warn("Error closing the stream " + fReader);
            }
        }
    }

    /**
     * Read /proc/net/dev file, parse and calculate amount
     * of bytes read and written through the network by main enp.
     */
    protected void readProcMainNetInfoFile(String computeType) {

        // Read "/proc/net/dev" file
        BufferedReader in;
        InputStreamReader fReader;
        try {
            fReader = new InputStreamReader(
                    Files.newInputStream(Paths.get(procfsNetFile)),
                    Charset.forName("UTF-8"));
            in = new BufferedReader(fReader);
        } catch (IOException f) {
            return;
        }

        Matcher mat;
        try {
            String str = in.readLine();
            while (str != null) {
                mat = PROCFS_NETFILE_FORMAT.matcher(str);
                if (mat.find()) {
                    assert mat.groupCount() >= 16;

                    // ignore loopback interfaces
                    if (mat.group(1).equals("lo")) {
                        str = in.readLine();
                        continue;
                    }
                    String name = mat.group(1);
                    long read = Long.parseLong(mat.group(2));
                    long write = Long.parseLong(mat.group(10));
                    long speedLimit = getNetworkSpeedLimit(name);
                    if (speedLimit <= 0) {
                        str = in.readLine();
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
                str = in.readLine();
            }
        } catch (IOException io) {
            LOG.warn("Error reading the stream " + io);
        } finally {
            // Close the streams
            try {
                fReader.close();
                try {
                    in.close();
                } catch (IOException i) {
                    LOG.warn("Error closing the stream " + in);
                }
            } catch (IOException i) {
                LOG.warn("Error closing the stream " + fReader);
            }
        }
    }

    /**
     * Read /proc/diskstats file, parse and calculate amount
     * of bytes read and written from/to disks.
     */
    protected void readProcDisksInfoFile(String[] paths) {

        numDisksBytesRead = 0L;
        numDisksBytesWritten = 0L;

        // Read "/proc/diskstats" file
        Matcher mat;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(procfsDisksFile), StandardCharsets.UTF_8))){
            numDisks = 0;
            if (paths != null && (!Arrays.equals(lastPaths, paths) || mountList.size() != paths.length)) {
                mountList = new ArrayList<>(paths.length);
                for (String path : paths) {
                    try {
                        DF df = new DF(new File(path), DF_INTERVAL_DEFAULT);
                        mountList.add(df.getFilesystem());
                    } catch (Throwable t) {
                        // path may not exist for unhealthy disks etc.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Read disk path " + path + " error.", t);
                        }
                    }
                }
                lastPaths = paths;
            }

            String str = in.readLine();
            long totalIoPerDisk = 0;
            while (str != null) {
                mat = PROCFS_DISKSFILE_FORMAT.matcher(str);
                if (mat.find()) {
                    String diskName = mat.group(4);
                    assert diskName != null;
                    // ignore loop or ram partitions
                    if (diskName.contains("loop") || diskName.contains("ram")) {
                        str = in.readLine();
                        continue;
                    }

                    numDisks++;
                    totalIoPerDisk += Long.parseLong(mat.group(14));

                    Integer sectorSize;
                    synchronized (perDiskSectorSize) {
                        sectorSize = perDiskSectorSize.get(diskName);
                        if (null == sectorSize) {
                            // retrieve sectorSize
                            // if unavailable or error, assume 512
                            sectorSize = readDiskBlockInformation(diskName, 512);
                            perDiskSectorSize.put(diskName, sectorSize);
                        }
                    }

                    String sectorsRead = mat.group(7);
                    String sectorsWritten = mat.group(11);
                    if (null == sectorsRead || null == sectorsWritten) {
                        return;
                    }
                    numDisksBytesRead += Long.parseLong(sectorsRead) * sectorSize;
                    numDisksBytesWritten += Long.parseLong(sectorsWritten) * sectorSize;
                }
                str = in.readLine();
            }

            diskIoTimeTracker.updateElapsedJiffies(
                    BigInteger.valueOf(totalIoPerDisk),
                    getCurrentTime());
        } catch (IOException e) {
            LOG.warn("Error reading the stream " + procfsDisksFile, e);
        }
    }

    /**
     * Read /sys/block/diskName/queue/hw_sector_size file, parse and calculate
     * sector size for a specific disk.
     * @return sector size of specified disk, or defSector
     */
    int readDiskBlockInformation(String diskName, int defSector) {

        assert perDiskSectorSize != null && diskName != null;

        String procfsDiskSectorFile =
                "/sys/block/" + diskName + "/queue/hw_sector_size";

        BufferedReader in;
        try {
            in = new BufferedReader(new InputStreamReader(
                    Files.newInputStream(Paths.get(procfsDiskSectorFile)),
                    Charset.forName("UTF-8")));
        } catch (IOException f) {
            return defSector;
        }

        Matcher mat;
        try {
            String str = in.readLine();
            while (str != null) {
                mat = PROCFS_DISKSECTORFILE_FORMAT.matcher(str);
                if (mat.find()) {
                    String secSize = mat.group(1);
                    if (secSize != null) {
                        return Integer.parseInt(secSize);
                    }
                }
                str = in.readLine();
            }
            return defSector;
        } catch (IOException|NumberFormatException e) {
            LOG.warn("Error reading the stream " + procfsDiskSectorFile, e);
            return defSector;
        } finally {
            // Close the streams
            try {
                in.close();
            } catch (IOException e) {
                LOG.warn("Error closing the stream " + procfsDiskSectorFile, e);
            }
        }
    }

    public long getPhysicalMemorySize() {
        readProcMemInfoFile();
        return (ramSize
                - hardwareCorruptSize
                - (hugePagesTotal * hugePageSize)) * 1024;
    }


    public long getVirtualMemorySize() {
        return getPhysicalMemorySize() + (swapSize * 1024);
    }

    public long getAvailablePhysicalMemorySize() {
        readProcMemInfoFile(true);
        long inactive = inactiveFileSize != -1
                ? inactiveFileSize
                : inactiveSize;
        return (ramSizeFree + inactive) * 1024;
    }


    public long getAvailableVirtualMemorySize() {
        return getAvailablePhysicalMemorySize() + (swapSizeFree * 1024);
    }

    public int getNumProcessors() {
        readProcCpuInfoFile();
        return numProcessors;
    }

    public int getNumCores() {
        readProcCpuInfoFile();
        return numCores;
    }

    public long getCpuFrequency() {
        readProcCpuInfoFile();
        return cpuFrequency;
    }

    public long getCumulativeCpuTime() {
        readProcStatFile();
        return cpuTimeTracker.getCumulativeTime();
    }

    public float getCpuUsagePercentage() {
        readProcStatFile();
        float overallCpuUsage = cpuTimeTracker.getUsagePercent();
        if (overallCpuUsage != TimeTracker.UNAVAILABLE) {
            overallCpuUsage = overallCpuUsage / getNumProcessors();
        }
        return overallCpuUsage;
    }

    public float getIoUsagePercentage(String[] paths) {
        readProcDisksInfoFile(paths);
        float overallIoUsage = diskIoTimeTracker.getUsagePercent();
        if (overallIoUsage != TimeTracker.UNAVAILABLE && numDisks != 0) {
            overallIoUsage = overallIoUsage / numDisks;
        }
        return overallIoUsage;
    }

    /**
     * getNetUsagePercentage
     * @param computeType send,receive,average,max
     * @return NetUsagePercentage
     */
    public float getNetUsagePercentage(String computeType) {
        readProcMainNetInfoFile(computeType);

        float maxReceiveUsagePercentage = -100;
        float maxSendUsagePercentage = -100;
        float maxTotalUsagePercentage = -200;
        for (String networkInterfaceName : networkSpeedLimitMap.keySet()) {
            float receive = -100;
            float send = -100;
            if (netIoReceiveTimeTrackerMap.containsKey(networkInterfaceName)) {
                receive = netIoReceiveTimeTrackerMap.get(networkInterfaceName).getUsagePercent();
            }
            if (netIoSendTimeTrackerMap.containsKey(networkInterfaceName)) {
                send = netIoSendTimeTrackerMap.get(networkInterfaceName).getUsagePercent();
            }
            maxReceiveUsagePercentage = Math.max(maxReceiveUsagePercentage, receive);
            maxSendUsagePercentage = Math.max(maxSendUsagePercentage, send);
            maxTotalUsagePercentage = Math.max(maxTotalUsagePercentage, receive + send);
        }

        switch (computeType) {
            case NM_RESOURCE_MON_NET_COMPUTE_TYPE_SEND:
                return maxSendUsagePercentage;
            case NM_RESOURCE_MON_NET_COMPUTE_TYPE_RECEIVE:
                return maxReceiveUsagePercentage;
            case NM_RESOURCE_MON_NET_COMPUTE_TYPE_AVERAGE:
                return (maxTotalUsagePercentage) / 2.0f;
            default:
                return Math.max(maxSendUsagePercentage, maxReceiveUsagePercentage);
        }
    }

    public float getPhyMemUsagePercentage() {
        return (1.0f - (float) getAvailablePhysicalMemorySize() / (float) getPhysicalMemorySize()) * 100;
    }

    public long getNetworkBytesRead() {
        readProcNetInfoFile();
        return numNetBytesRead;
    }

    public long getNetworkBytesWritten() {
        readProcNetInfoFile();
        return numNetBytesWritten;
    }

    public long getStorageBytesRead(String[] paths) {
        readProcDisksInfoFile(paths);
        return numDisksBytesRead;
    }

    public long getStorageBytesWritten(String[] paths) {
        readProcDisksInfoFile(paths);
        return numDisksBytesWritten;
    }

    public long computeNetworkTimeMs(long transBytes, long speedMB) {
        return transBytes * 1000 / speedMB / 1024 / 1024;
    }

    protected long getNetworkSpeedLimit(String networkInterfaceName) {
        if (!networkSpeedLimitMap.containsKey(networkInterfaceName)) {
            readNetworkSpeedLimit(networkInterfaceName);
        }
        if (!netIoReceiveTimeTrackerMap.containsKey(networkInterfaceName)) {
            netIoReceiveTimeTrackerMap.put(networkInterfaceName, new TimeTracker("net_receive", jiffyLengthInMillis));
            netIoSendTimeTrackerMap.put(networkInterfaceName, new TimeTracker("net_send", jiffyLengthInMillis));
        }
        return networkSpeedLimitMap.getOrDefault(networkInterfaceName, -1L);
    }

    protected void readNetworkSpeedLimit(String networkInterfaceName) {
        long speedMB = -1L;
        Matcher mat;

        try {
            ShellCommandExecutor shellExecutorClk = new ShellCommandExecutor(
                    new String[]{"ethtool", networkInterfaceName});
            shellExecutorClk.execute();
            for (String line : shellExecutorClk.getOutput().split("\n")) {
                mat = NETWORK_SPEED_LIMIT.matcher(line);
                if (mat.find()) {
                    speedMB = Long.parseLong(mat.group(2)) / 8;
                    networkSpeedLimitMap.put(networkInterfaceName, speedMB);
                    break;
                }
            }
        } catch (IOException e) {
            LOG.warn("readNetworkSpeedLimit from {} failed for :{}", networkInterfaceName, e);
        }
    }

    public String roundTime(int heartbeatInterval) {
        LocalDateTime time = LocalDateTime.now();
        int intervalSeconds = heartbeatInterval / 1000;
        // when intervalSeconds in (1,3600] do round time
        // such as intervalSeconds is 10, now is 10:00:03, after round time is 10:00:00 for ui present
        // such as intervalSeconds is 600, now is 10:12:03, after round time is 10:10:00 for ui present
        if (intervalSeconds > 1 && intervalSeconds <= HOUR_SECONDS) {
            int second = time.getSecond();
            int minute = time.getMinute();
            second = second + minute * MINUTE_SECONDS;
            int roundedSecond = second / intervalSeconds * intervalSeconds;
            minute = roundedSecond / MINUTE_SECONDS;
            second = roundedSecond % MINUTE_SECONDS;
            time = time.withMinute(minute).withSecond(second);
        }
        return time.format(dateTimeFormatter);
    }

    public String formatUsage(float usage){
        return usageFormatter.format(usage);
    }

    public Float formatFloatUsage(float usage){
        return Float.valueOf(usageFormatter.format(usage));
    }
}
