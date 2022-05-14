package org.apache.hadoop.hive.ql.omnidata.status;

/**
 * OmniData node status
 *
 * @since 2021-03
 */
public class NdpStatusInfo {
    private String datanodeHost;

    private String version;

    private double threshold;

    private int runningTasks;

    private int maxTasks;

    public NdpStatusInfo(String datanodeHost, String version, double threshold, int runningTasks, int maxTasks) {
        this.datanodeHost = datanodeHost;
        this.version = version;
        this.threshold = threshold;
        this.runningTasks = runningTasks;
        this.maxTasks = maxTasks;
    }

    public NdpStatusInfo() {
    }

    public String getDatanodeHost() {
        return datanodeHost;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public int getRunningTasks() {
        return runningTasks;
    }

    public int getMaxTasks() {
        return maxTasks;
    }
}
