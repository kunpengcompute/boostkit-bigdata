package com.huawei.omniruntime.flink.runtime.api.graph.json;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.configuration.StreamConfigHelper;

import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TaskInformationPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class TaskInformationPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(TaskInformationPOJO.class);

    StreamConfigPOJO streamConfig;
    List<StreamConfigPOJO> chainedConfig;
    private String taskName;

    /**
     * The number of subtasks for this operator.
     */
    private int numberOfSubtasks;

    /**
     * The maximum parallelism == number of key groups.
     */
    private int maxNumberOfSubtasks;
    private int indexOfSubtask;

    private String stateBackend;

    private String[] rocksdbStorePaths = new String[0];

    // Default constructor
    public TaskInformationPOJO() {
    }

    public TaskInformationPOJO(TaskInformation taskInformation, ClassLoader cl, int indexOfSubtask) {
        this.taskName = taskInformation.getTaskName();
        this.numberOfSubtasks = taskInformation.getNumberOfSubtasks();
        this.maxNumberOfSubtasks = taskInformation.getMaxNumberOfSubtasks();
        this.indexOfSubtask = indexOfSubtask;
        StreamConfig tempStreamConfig = new StreamConfig(taskInformation.getTaskConfiguration());
        getBackend(tempStreamConfig);
        this.streamConfig = new StreamConfigPOJO(tempStreamConfig, cl);
        LOG.info("before OperatorChainDescriptorHelper.retrieveOperatorChain");
        LOG.info("after OperatorChainDescriptorHelper.retrieveOperatorChain");

        this.chainedConfig = new ArrayList<>(StreamConfigHelper.retrieveChainedConfig(tempStreamConfig, cl).values());
    }

    // Full argument constructor
    public TaskInformationPOJO(String taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                               StreamConfigPOJO streamConfig,
                               List<StreamConfigPOJO> chainedConfig) {
        this.taskName = taskName;
        this.numberOfSubtasks = numberOfSubtasks;
        this.maxNumberOfSubtasks = maxNumberOfSubtasks;
        this.indexOfSubtask = indexOfSubtask;
        this.streamConfig = streamConfig;
        this.chainedConfig = chainedConfig;
    }

    private void getBackend(StreamConfig tempStreamConfig) {
        StateBackend backend = tempStreamConfig.getStateBackend(Thread.currentThread().getContextClassLoader());
        stateBackend = backend == null ? "HashMapStateBackend" : backend.getName();
        if (!stateBackend.equals("EmbeddedRocksDBStateBackend")) {
            return;
        }
        try {
            Class<?> backendClass = backend.getClass();
            Field[] fields = backendClass.getDeclaredFields();
            for (Field field : fields) {
                String name = field.getName();
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                if (!name.equals("localRocksDbDirectories")) {
                    continue;
                }
                Object value = field.get(backend);
                File[] localRocksDbDirectories = (File[]) value;
                rocksdbStorePaths = getDbStoragePaths(localRocksDbDirectories);
                break;
            }
        } catch (IllegalAccessException ex) {
            LOG.warn("get rocksdb storePath failed", ex);
        }
    }

    private String[] getDbStoragePaths(File[] localRocksDbDirectories) {
        if (localRocksDbDirectories == null) {
            return new String[0];
        } else {
            String[] paths = new String[localRocksDbDirectories.length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = localRocksDbDirectories[i].toString();
            }
            return paths;
        }
    }


    // Getters and setters
    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public void setNumberOfSubtasks(int numberOfSubtasks) {
        this.numberOfSubtasks = numberOfSubtasks;
    }

    public int getMaxNumberOfSubtasks() {
        return maxNumberOfSubtasks;
    }

    public void setMaxNumberOfSubtasks(int maxNumberOfSubtasks) {
        this.maxNumberOfSubtasks = maxNumberOfSubtasks;
    }

    public StreamConfigPOJO getStreamConfig() {
        return streamConfig;
    }

    public void setStreamConfig(StreamConfigPOJO streamConfig) {
        this.streamConfig = streamConfig;
    }

    public int getIndexOfSubtask() {
        return indexOfSubtask;
    }

    public void setIndexOfSubtask(int indexOfSubtask) {
        this.indexOfSubtask = indexOfSubtask;
    }

    public List<StreamConfigPOJO> getChainedConfig() {
        return chainedConfig;
    }

    public void setChainedConfig(List<StreamConfigPOJO> chainedConfig) {
        this.chainedConfig = chainedConfig;
    }

    public String getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(String stateBackend) {
        this.stateBackend = stateBackend;
    }

    public String[] getRocksdbStorePaths() {
        return rocksdbStorePaths;
    }

    public void setRocksdbStorePaths(String[] rocksdbStorePaths) {
        this.rocksdbStorePaths = rocksdbStorePaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof TaskInformationPOJO);
        TaskInformationPOJO that = (TaskInformationPOJO) o;
        return numberOfSubtasks == that.numberOfSubtasks
                && maxNumberOfSubtasks == that.maxNumberOfSubtasks
                && Objects.equals(taskName, that.taskName)
                && Objects.equals(streamConfig, that.streamConfig)
                && Objects.equals(chainedConfig, that.chainedConfig)
                && Objects.equals(indexOfSubtask, that.indexOfSubtask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                taskName,
                numberOfSubtasks,
                maxNumberOfSubtasks,
                indexOfSubtask,
                streamConfig,
                chainedConfig);
    }

    @Override
    public String toString() {
        return "TaskInformationPOJO{"
                + "taskName='" + taskName + '\''
                + ", numberOfSubtasks=" + numberOfSubtasks
                + ", maxNumberOfSubtasks=" + maxNumberOfSubtasks
                + ", indexOfSubtask=" + indexOfSubtask
                + ", streamConfig=" + streamConfig
                + ", chainedConfig=" + chainedConfig
                + '}';
    }
}
