package com.huawei.omniruntime.flink.runtime.taskexecutor;

import static org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils.generateJobManagerWorkingDirectoryFile;
import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskManagerServicesConfigurationPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResourceIDPOJO;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceSpec;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Optional;

/**
 * OmniTaskManagerServicesConfiguration
 *
 * @since 2025-04-27
 */
public class OmniTaskManagerServicesConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTaskManagerServices.class);


    /**
     * fromConfiguration
     *
     * @param configuration configuration
     * @param resourceID resourceID
     * @param memorySize memorySize
     * @param pageSize pageSize
     * @param numIoThreads numIoThreads
     * @param externalAddress externalAddress
     * @param localCommunicationOnly localCommunicationOnly
     * @return TaskManagerServicesConfigurationPOJO
     * @throws Exception Exception
     */
    public static TaskManagerServicesConfigurationPOJO fromConfiguration(
            Configuration configuration,
            ResourceID resourceID,
            MemorySize memorySize,
            int pageSize,
            int numIoThreads,
            String externalAddress,
            boolean localCommunicationOnly
    ) throws Exception {
        Method method = TaskExecutorResourceUtils
                .class
                .getDeclaredMethod("resourceSpecFromConfig", Configuration.class);
        method.setAccessible(true);
        checkState(method.invoke(null, configuration) instanceof TaskExecutorResourceSpec);
        TaskExecutorResourceSpec taskExecutorResourceSpec =
                (TaskExecutorResourceSpec) method.invoke(null, configuration);

        WorkingDirectory workingDirectory = WorkingDirectory.create(
                generateJobManagerWorkingDirectoryFile(configuration, resourceID));

        Duration requestSegmentsTimeout =
                Duration.ofMillis(
                        configuration.getLong(
                                NettyShuffleEnvironmentOptions
                                        .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
        long requestSegmentsTimeoutMillis = configuration.getLong(
                NettyShuffleEnvironmentOptions
                        .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS);
        long memorySizeInBytes = memorySize.getBytes();

        TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                TaskManagerServicesConfiguration.fromConfiguration(
                configuration,
                resourceID,
                externalAddress,
                localCommunicationOnly,
                taskExecutorResourceSpec,
                workingDirectory

        );

        Class<?> clazz = taskManagerServicesConfiguration.getClass();

        Method getBindAddressMethod = clazz.getDeclaredMethod("getBindAddress");

        getBindAddressMethod.setAccessible(true);
        checkState(getBindAddressMethod.invoke(taskManagerServicesConfiguration) instanceof InetAddress);
        InetAddress bindAddress = (InetAddress) getBindAddressMethod.invoke(taskManagerServicesConfiguration);

        NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        taskManagerServicesConfiguration.getConfiguration(),
                        taskManagerServicesConfiguration.getNetworkMemorySize(),
                        localCommunicationOnly,
                        bindAddress);
        return new TaskManagerServicesConfigurationPOJO(
                new ResourceIDPOJO(Optional.of(resourceID)),
                memorySizeInBytes,
                pageSize,
                requestSegmentsTimeoutMillis,
                numIoThreads,
                externalAddress,
                localCommunicationOnly,
                networkConfig);
    }
}
