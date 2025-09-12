package com.huawei.omniruntime.flink.runtime.execution;

import com.huawei.omniruntime.flink.runtime.shuffle.OmniShuffleEnvironment;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * OmniRuntimeEnvironment
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
public class OmniRuntimeEnvironment extends RuntimeEnvironment implements OmniEnvironment {
    private long nativeEnvironmentRef;
    private long nativeTaskRef;

    private OmniShuffleEnvironment shuffleEnvironment;

    public OmniRuntimeEnvironment(JobID jobId,
                                  JobVertexID jobVertexId,
                                  ExecutionAttemptID executionId,
                                  ExecutionConfig executionConfig,
                                  TaskInfo taskInfo,
                                  Configuration jobConfiguration,
                                  Configuration taskConfiguration,
                                  UserCodeClassLoader userCodeClassLoader,
                                  MemoryManager memManager,
                                  IOManager ioManager,
                                  BroadcastVariableManager bcVarManager,
                                  TaskStateManager taskStateManager,
                                  GlobalAggregateManager aggregateManager,
                                  AccumulatorRegistry accumulatorRegistry,
                                  TaskKvStateRegistry kvStateRegistry,
                                  InputSplitProvider splitProvider,
                                  Map<String, Future<Path>> distCacheEntries,
                                  ResultPartitionWriter[] writers,
                                  IndexedInputGate[] inputGates,
                                  TaskEventDispatcher taskEventDispatcher,
                                  CheckpointResponder checkpointResponder,
                                  TaskOperatorEventGateway operatorEventGateway,
                                  TaskManagerRuntimeInfo taskManagerInfo,
                                  TaskMetricGroup metrics,
                                  Task containingTask,
                                  ExternalResourceInfoProvider externalResourceInfoProvider,
                                  OmniShuffleEnvironment omniShuffleEnvironment, long nativeTaskRef) {
        super(jobId, jobVertexId, executionId, executionConfig, taskInfo, jobConfiguration, taskConfiguration,
                userCodeClassLoader, memManager, ioManager, bcVarManager, taskStateManager, aggregateManager,
                accumulatorRegistry, kvStateRegistry, splitProvider, distCacheEntries, writers, inputGates,
                taskEventDispatcher, checkpointResponder, operatorEventGateway, taskManagerInfo, metrics,
                containingTask,
                externalResourceInfoProvider);
        this.shuffleEnvironment = omniShuffleEnvironment;
        this.nativeTaskRef = nativeTaskRef;
        this.nativeEnvironmentRef = createNativeEnvironment(omniShuffleEnvironment.getNativeShuffleServiceRef());
    }

    @Override
    public long getNativeEnvironmentRef() {
        return nativeEnvironmentRef;
    }

    private native long createNativeEnvironment(long nativeShuffleServiceRef);
}
