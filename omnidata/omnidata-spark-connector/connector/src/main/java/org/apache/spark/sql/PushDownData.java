package org.apache.spark.sql;

/**
 * PushDownData
 *
 * @date 2021/3/23 20:30
 */
public class PushDownData {
    private String datanodeHost;
    private String version;
    private double threshold;
    private int runningTasks;
    private int maxTasks;

    public String getDatanodeHost() {
        return datanodeHost;
    }

    public void setDatanodeHost(String datanodeHost) {
        this.datanodeHost = datanodeHost;
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

    public void setRunningTasks(int runningTasks) {
        this.runningTasks = runningTasks;
    }

    public int getMaxTasks() {
        return maxTasks;
    }

    public void setMaxTasks(int maxTasks) {
        this.maxTasks = maxTasks;
    }
}

