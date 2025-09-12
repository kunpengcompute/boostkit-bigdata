/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.runtime.io.network.partition.RemoteDataFetcher;
import com.huawei.omniruntime.flink.runtime.metrics.groups.OmniTaskMetricGroup;
import com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import com.huawei.omniruntime.flink.runtime.tasks.NativeStreamTask;
import com.huawei.omniruntime.flink.streaming.api.graph.JobType;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.OmniRemoteInputChannel;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.OperatorEventDispatcherImpl;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;

/**
 * customized OmniTask for OmniRuntime
 *
 * @since 2025-02-16
 */
public class OmniTask extends Task {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTask.class);
    private static final String OMNI_SOURCE_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceStreamTask";
    private static final String SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask";
    private static final String SOURCE_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.SourceStreamTask";
    private static final String OMNI_SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniSourceOperatorStreamTask";
    private static final String OMNI_ONEINPUT_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniOneInputStreamTaskV2";
    private static final String OMNI_TWOINPUT_STREAM_TASK_CLASS_NAME =
            "com.huawei.omniruntime.flink.runtime.tasks.OmniTwoInputStreamTaskV2";
    private static final String ONEINPUT_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.OneInputStreamTask";
    private static final String TWOINPUT_STREAM_TASK_CLASS_NAME =
            "org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask";

    private long nativeTaskRef;
    private JobType jobType; // 0 default vanilla java task, 1 native sql task, 2 native datastream task. 3 future - hybrid java+cpp source task

    // dup fields of the Task as those field are prviate in parent class Task
    private JobInformation __jobInformation;
    private TaskInformation __taskInformation;
    private RemoteDataFetcher remoteDataFetcher;
    private OperatorEventDispatcherImpl operatorEventDispatcher;
    private Map<OperatorID, OperatorEventHandler> operatorEventHandlers;

    /**
     * The name of the alias class for omnistream
     */
    private String aliasOfInvokableClass;
    private long nativeTaskMetricGroupRef;
    private OmniTaskMetricGroup omniTaskMetricGroup;

    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     *
     * @param jobInformation
     * @param taskInformation
     * @param executionAttemptID
     * @param slotAllocationId
     * @param resultPartitionDeploymentDescriptors
     * @param inputGateDeploymentDescriptors
     * @param memManager
     * @param ioManager
     * @param shuffleEnvironment
     * @param kvStateService
     * @param bcVarManager
     * @param taskEventDispatcher
     * @param externalResourceInfoProvider
     * @param taskStateManager
     * @param taskManagerActions
     * @param inputSplitProvider
     * @param checkpointResponder
     * @param operatorCoordinatorEventGateway
     * @param aggregateManager
     * @param classLoaderHandle
     * @param fileCache
     * @param taskManagerConfig
     * @param metricGroup
     * @param partitionProducerStateChecker
     * @param executor
     */
    public OmniTask(JobInformation jobInformation, TaskInformation taskInformation,
            ExecutionAttemptID executionAttemptID, AllocationID slotAllocationId,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors, MemoryManager memManager,
            IOManager ioManager, ShuffleEnvironment<?, ?> shuffleEnvironment, KvStateService kvStateService,
            BroadcastVariableManager bcVarManager, TaskEventDispatcher taskEventDispatcher,
            ExternalResourceInfoProvider externalResourceInfoProvider, TaskStateManager taskStateManager,
            TaskManagerActions taskManagerActions, InputSplitProvider inputSplitProvider,
            CheckpointResponder checkpointResponder, TaskOperatorEventGateway operatorCoordinatorEventGateway,
            GlobalAggregateManager aggregateManager, LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            FileCache fileCache, TaskManagerRuntimeInfo taskManagerConfig, @Nonnull TaskMetricGroup metricGroup,
            PartitionProducerStateChecker partitionProducerStateChecker, Executor executor) {
        super(jobInformation, taskInformation, executionAttemptID, slotAllocationId,
                resultPartitionDeploymentDescriptors, inputGateDeploymentDescriptors, memManager, ioManager,
                shuffleEnvironment, kvStateService, bcVarManager, taskEventDispatcher, externalResourceInfoProvider,
                taskStateManager, taskManagerActions, inputSplitProvider, checkpointResponder,
                operatorCoordinatorEventGateway, aggregateManager, classLoaderHandle, fileCache, taskManagerConfig,
                metricGroup, partitionProducerStateChecker, executor);
        this.__taskInformation = taskInformation;
        this.__jobInformation = jobInformation;
        this.jobType = JobType.NULL;
    }

    /**
     * The core work method that bootstraps the task and executes its code.
     */
    @Override
    public void run() {
        try {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }
    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        initState();
        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------
            Environment env = prepareEnvironment(distributedCacheEntries);
            invokable = createInitAndInvokeTask(env);
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------
            t = preProcessException(t);
            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                throwableHandle(t, invokable);
            } catch (Throwable tt) {
                String message = String.format("FATAL - exception in exception handler of task %s (%s).",
                        taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);
                if (remoteDataFetcher != null) {
                    remoteDataFetcher.finishRunning();
                }
                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                this.invokable = null;
                // free the network resources
                releaseResources();

                // free memory resources
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                String message = String.format("FATAL - exception in resource cleanup of task %s (%s).",
                        taskNameWithSubtask, executionId);
                LOG.error(message, t);
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            try {
                metrics.close();
                if (omniTaskMetricGroup != null) {
                    omniTaskMetricGroup.close();
                }
            } catch (Throwable t) {
                LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId,
                        t);
            }
        }
    }

    private void initState() {
        while (true) {
            ExecutionState current = this.executionState;
            if (current == ExecutionState.CREATED) {
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                    // we were immediately failed. tell the TaskManager that we reached our final state
                notifyFinalStateAndCloseMetrics();
                return;
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    notifyFinalStateAndCloseMetrics();
                    return;
                }
            } else {
                if (metrics != null) {
                    metrics.close();
                }
                throw new IllegalStateException
                        ("Invalid state for beginning of operation of task " + this + '.');
            }
        }
    }
    private void notifyFinalStateAndCloseMetrics() {
        notifyFinalState();
        if (metrics != null) {
            metrics.close();
        }
    }

    private ExecutionConfig beforePrepareEnvironment
            (Map<String, Future<Path>> distributedCacheEntries) throws Exception {
        LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
        FileSystemSafetyNet.initializeSafetyNetForThread();

        // first of all, get a user-code classloader
        // this may involve downloading the job's JAR files and/or classes
        LOG.info("Loading JAR files for task {}.", this);

        userCodeClassLoader = createUserCodeClassloader();
        final ExecutionConfig executionConfig =
                serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());

        if (executionConfig.getTaskCancellationInterval() >= 0) {
            // override task cancellation interval from Flink config if set in ExecutionConfig
            taskCancellationInterval = executionConfig.getTaskCancellationInterval();
        }

        if (executionConfig.getTaskCancellationTimeout() >= 0) {
            // override task cancellation timeout from Flink config if set in ExecutionConfig
            taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
        }

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        // ----------------------------------------------------------------
        // register the task with the network stack
        // this operation may fail if the system does not have enough
        // memory to run the necessary data exchanges
        // the registration must also strictly be undone
        // ----------------------------------------------------------------

        LOG.debug("Registering task at network: {}.", this);
        // action 1, natvie should do similary operation
        setupPartitionsAndGates(partitionWriters, inputGates);
        if (jobType == JobType.SQL) {
            bindNativeTaskRefToResultPartition(nativeTaskRef, partitionWriters);
        }
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
        }
        // action 1 end

        // next, kick off the background copying of files for the distributed cache
        try {
            for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                    DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
                distributedCacheEntries.put(entry.getKey(), cp);
            }
        } catch (Exception e) {
            throw new Exception(String.format("Exception while adding files to distributed cache of task %s (%s).",
                    taskNameWithSubtask, executionId), e);
        }

        if (isCanceledOrFailed()) {
            throw new CancelTaskException();
        }

        return executionConfig;
    }

    private Environment prepareEnvironment(Map<String, Future<Path>> distributedCacheEntries) throws Exception {
        ExecutionConfig executionConfig = beforePrepareEnvironment(distributedCacheEntries);
        // ----------------------------------------------------------------
        //  call the user code initialization methods
        // ----------------------------------------------------------------

        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

        Environment env = new RuntimeEnvironment(jobId, vertexId, executionId, executionConfig, taskInfo,
                jobConfiguration, taskConfiguration, userCodeClassLoader, memoryManager, ioManager,
                broadcastVariableManager, taskStateManager, aggregateManager, accumulatorRegistry, kvStateRegistry,
                inputSplitProvider, distributedCacheEntries, partitionWriters, inputGates, taskEventDispatcher,
                checkpointResponder, operatorCoordinatorEventGateway, taskManagerConfig, metrics, this,
                externalResourceInfoProvider);

        // Make sure the user code classloader is accessible thread-locally.
        // We are setting the correct context class loader before instantiating the invokable
        // so that it is available to the invokable during its entire lifetime.
        executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

        StreamConfig streamConfig = new StreamConfig(taskConfiguration);
        InternalOperatorMetricGroup operator = env.getMetricGroup().getOrAddOperator(streamConfig.getOperatorID(),
                streamConfig.getOperatorName());
        // invokale Omni Source Operator Stream Task

        // When constructing invokable, separate threads can be constructed and thus should be
        // monitored for system exit (in addition to invoking thread itself monitored below).
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        LOG.info("the task's invokable is {} ", nameOfInvokableClass);
        return env;
    }

    private void initAliasOfInvokableClass() {
        LOG.info("the task's taskType is {} ", jobType.getValue());
        if (jobType == JobType.SQL) {
            StreamConfig streamConfig = new StreamConfig(taskConfiguration);
            StreamOperatorFactory<?> streamOperatorFactory =
                streamConfig.getStreamOperatorFactory(userCodeClassLoader.asClassLoader());
            boolean isKafkaSource = streamOperatorFactory instanceof SourceOperatorFactory
                && ReflectionUtils.retrievePrivateField(
                streamOperatorFactory, "source").getClass().getSimpleName().equals("KafkaSource");
            if (isKafkaSource && nameOfInvokableClass.equals(SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(SOURCE_OPERATOR_STREAM_TASK_CLASS_NAME)
                || nameOfInvokableClass.equals(SOURCE_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_SOURCE_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(ONEINPUT_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_ONEINPUT_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            } else if (nameOfInvokableClass.equals(TWOINPUT_STREAM_TASK_CLASS_NAME)) {
                aliasOfInvokableClass = OMNI_TWOINPUT_STREAM_TASK_CLASS_NAME;
                LOG.info(" Alias Invokable Class is {}", nameOfInvokableClass);
            }
        }
    }

    private TaskInvokable createInitAndInvokeTask(Environment env) throws Throwable {
        initAliasOfInvokableClass();
        TaskInvokable invokable;
        try {
            // now load and instantiate the task's invokable code
            invokable =
                    loadAndInstantiateInvokable(
                            userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }

       // ----------------------------------------------------------------
        //  actual task core work
        // ----------------------------------------------------------------


        // we must make strictly sure that the invokable is accessible to the cancel() call
        // by the time we switched to running.
        this.invokable = invokable;

        // invoke will start the mailbox loop
        // before this invoke, the native stream task and all setup should be ready
        // OmniStreamTask binding to native stream task should be ready as wll

        long nativeStreamTask = 0L;
        if (jobType == JobType.SQL) {
            nativeStreamTask = setupStreamTaskBeforeInvoke(this.nativeTaskRef, this.aliasOfInvokableClass);

            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            // restore original task first
            nativeTaskMetricGroupRef = createNativeTaskMetricGroup(nativeTaskRef);
            // register omni metrics
            omniTaskMetricGroup = registerOmniTaskMetrics();

            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                throw new CancelTaskException();
            }

            taskManagerActions.updateTaskExecutionState(new TaskExecutionState(executionId,
                    ExecutionState.INITIALIZING));

            // make sure the user code classloader is accessible thread-locally
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // create RemoteDataFetcher for remote input channelsE

            remoteDataFetcher = createAndStartRemoteDataFetcher(inputGates);

            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }
            taskManagerActions.updateTaskExecutionState(new TaskExecutionState(executionId, ExecutionState.RUNNING));
            // call native restore and invoke before java
            long status = doRunRestoreNativeTask(nativeTaskRef, nativeStreamTask);
            registerEventDispatcher((StreamTask<?, ?>) invokable);
            status = doRunInvokeNativeTask(nativeTaskRef, nativeStreamTask);
        } else {
            if (jobType == JobType.STREAM) {
                ((NativeStreamTask) invokable).binkNativeTaskAddress(nativeTaskRef);
            }
            restoreAndInvoke(invokable);
            // mailbox loop ended

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------

            // finish the produced partitions. if this fails, we consider the execution failed.
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                if (partitionWriter != null) {
                    partitionWriter.finish();
                }
            }
        }

        // Task (either java or cpp task) ended

        // try to mark the task as finished
        // if that fails, the task was canceled/failed in the meantime
        if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
            throw new CancelTaskException();
        }
        return invokable;
    }

    @Override
    public void cancelExecution() {
        super.cancelExecution();
        if (jobType.equals(JobType.SQL)
            && nameOfInvokableClass.equals("org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask")) {
            cancelTask(nativeTaskRef);
        }
    }

    private void throwableHandle(Throwable t, TaskInvokable invokable) {
        while (true) {
            ExecutionState current = this.executionState;

            if (current == ExecutionState.RUNNING || current == ExecutionState.INITIALIZING
                || current == ExecutionState.DEPLOYING) {
                boolean isBreak = doNonCancelAndNonFailThrowableHandle(t, invokable, current);
                if (isBreak) {
                    break;
                }
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(current, ExecutionState.CANCELED)) {
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                // in state failed already, no transition necessary any more
                break;
            } else if (transitionState(current, ExecutionState.FAILED, t)) {
                LOG.error("Unexpected state in task {} ({}) during an exception: {}.",
                        taskNameWithSubtask, executionId, current);
                break;
            }
            // else fall through the loop and
        }
    }

    private boolean doNonCancelAndNonFailThrowableHandle(Throwable t, TaskInvokable invokable, ExecutionState current) {
        if (ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
            if (transitionState(current, ExecutionState.CANCELED, t)) {
                cancelInvokable(invokable);
                return true;
            }
        } else {
            if (transitionState(current, ExecutionState.FAILED, t)) {
                cancelInvokable(invokable);
                return true;
            }
        }
        return false;
    }

    @Override
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt) throws FlinkException {
        if (jobType != JobType.SQL) {
            super.deliverOperatorEvent(operator, evt);
            return;
        }
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        if (invokable == null
            || (currentState != ExecutionState.RUNNING
            && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            String desc = null;
            try {
                OperatorEvent operatorEvent = evt.deserializeValue(userCodeClassLoader.asClassLoader());
                desc = eventToJsonString(operatorEvent);
                dispatchOperatorEvent(nativeTaskRef, operator.toString(), desc);
            } catch (IOException | ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                if (getExecutionState() == ExecutionState.RUNNING
                    || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException flinkException = new FlinkException("Error while handling operator event", e);
                    failExternally(flinkException);
                    throw flinkException;
                }
            }
        }
    }

    // The operator event is triggered post-operator initialization
    // We split the native process into two stages,
    // with operator event registration inserted between them.
    private void registerEventDispatcher(StreamTask<?, ?> invokable) {
        operatorEventDispatcher = new OperatorEventDispatcherImpl(
            userCodeClassLoader.asClassLoader(), operatorCoordinatorEventGateway);
        Map<Integer, StreamConfig> transitiveChainedTaskConfigsWithSelf =
            invokable.getConfiguration().getTransitiveChainedTaskConfigsWithSelf(userCodeClassLoader.asClassLoader());
        //
        for (Map.Entry<Integer, StreamConfig> entry : transitiveChainedTaskConfigsWithSelf.entrySet()) {
            StreamConfig streamConfig = entry.getValue();
            if (streamConfig == null) {
                continue;
            }
            StreamOperatorFactory<?> streamOperatorFactory =
                streamConfig.getStreamOperatorFactory(userCodeClassLoader.asClassLoader());
            boolean isKafkaSource = streamOperatorFactory instanceof SourceOperatorFactory
                && ReflectionUtils.retrievePrivateField(
                streamOperatorFactory, "source").getClass().getSimpleName().equals("KafkaSource");
            // Additional operators(e.g. CollectSinkOperator) can perform post-initialization operations at this stage
            if (isKafkaSource) {
                OperatorID operatorID = streamConfig.getOperatorID();
                operatorEventDispatcher.getOperatorEventGateway(operatorID).sendEventToCoordinator(
                    new ReaderRegistrationEvent(
                        getTaskInfo().getIndexOfThisSubtask(),
                        taskManagerConfig.getTaskManagerExternalAddress())
                );
            }
        }
    }

    private List<String> convertToHexStringList(ArrayList<byte[]> splits) {
        List<String> hexStringList = new ArrayList<>();
        for (byte[] bytes : splits) {
            StringBuilder hexString = new StringBuilder();
            for (byte b : bytes) {
                String hex = Integer.toHexString(0xFF & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            hexStringList.add(hexString.toString());
        }
        return hexStringList;
    }

    private String eventToJsonString(OperatorEvent event) throws NoSuchFieldException, IllegalAccessException,
        JsonProcessingException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        Map<String, Object> fieldMap = new LinkedHashMap<>();
        jsonMap.put("field", fieldMap);
        if (event instanceof WatermarkAlignmentEvent) {
            jsonMap.put("type", "WatermarkAlignmentEvent");
            fieldMap.put("maxWatermark", ((WatermarkAlignmentEvent) event).getMaxWatermark());
        } else if (event instanceof AddSplitEvent) {
            jsonMap.put("type", "AddSplitEvent");
            Field serializerVersion = AddSplitEvent.class.getDeclaredField("serializerVersion");
            Field splits = AddSplitEvent.class.getDeclaredField("splits");
            serializerVersion.setAccessible(true);
            splits.setAccessible(true);
            fieldMap.put("serializerVersion", serializerVersion.get(event));
            ArrayList<byte[]> splitList = (ArrayList<byte[]>) splits.get(event);
            List<String> hexStringList = convertToHexStringList(splitList);
            fieldMap.put("splits", hexStringList);
        } else if (event instanceof SourceEventWrapper) {
            // TODO : currently, do not support HybridSourceReader here
            jsonMap.put("type", "SourceEventWrapper");
        } else if (event instanceof NoMoreSplitsEvent) {
            jsonMap.put("type", "NoMoreSplitsEvent");
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
        return objectMapper.writeValueAsString(jsonMap);
    }

    public void bindNativeTask(long nativeTaskAddress) {
        this.nativeTaskRef = nativeTaskAddress;
        bindNativeTaskRefToResultPartition(nativeTaskRef, partitionWriters);
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    private RemoteDataFetcher createAndStartRemoteDataFetcher(IndexedInputGate[] inputGates) {
        List<OmniRemoteInputChannel> remoteInputChannels = new ArrayList<>();
        for (IndexedInputGate inputGate : inputGates) {
            InputGateWithMetrics inputGateWithMetrics = (InputGateWithMetrics) inputGate;

            int numberOfChannels = inputGateWithMetrics.getNumberOfInputChannels();

            for (int i = 0; i < numberOfChannels; i++) {
                InputChannel inputChannel = inputGateWithMetrics.getChannel(i);
                setRecoverInputStateFutureCompleted((RecoveredInputChannel) inputChannel);
            }
            convertRemoteRecoveryChannelToNormal(inputGateWithMetrics);

            for (int i = 0; i < numberOfChannels; i++) {
                InputChannel inputChannel = inputGateWithMetrics.getChannel(i);
                if (inputChannel instanceof RemoteInputChannel) {
                    RemoteInputChannel remoteInputChannel = (RemoteInputChannel) inputChannel;
                    remoteInputChannels.add(new OmniRemoteInputChannel(remoteInputChannel));
                }
            }
        }

        if (remoteInputChannels.size() > 0) {
            RemoteDataFetcher remoteDataFetcher = new RemoteDataFetcher(remoteInputChannels, nativeTaskRef,
                    this.getTaskInfo().getTaskName());
            Thread thread = new Thread(remoteDataFetcher);
            thread.start();
            return remoteDataFetcher;
        }
        return null;
    }

    private void setRecoverInputStateFutureCompleted(RecoveredInputChannel recoveredInputChannel) {
        Class clazz = recoveredInputChannel.getClass().getSuperclass();
        try {
            Method method = clazz.getDeclaredMethod("getStateConsumedFuture");
            method.setAccessible(true);
            CompletableFuture future = (CompletableFuture) method.invoke(recoveredInputChannel);
            future.complete(null);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void convertRemoteRecoveryChannelToNormal(InputGateWithMetrics inputGateWithMetrics) {
        try {
            Field inputGateField = InputGateWithMetrics.class.getDeclaredField("inputGate");
            inputGateField.setAccessible(true);
            SingleInputGate singleInputGate = (SingleInputGate) inputGateField.get(inputGateWithMetrics);
            singleInputGate.convertRecoveredInputChannels();
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void bindNativeTaskRefToResultPartition(long nativeTaskRef,
            ResultPartitionWriter[] consumableNotifyingPartitionWriters) {
        for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            partition.setNativeTaskRef(nativeTaskRef);
        }
    }

    private void stopOmniCreditBasedSequenceNumberingViewReader() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            partition.stopOmniCreditBasedSequenceNumberingViewReader();
        }
    }
    private OmniTaskMetricGroup registerOmniTaskMetrics() {
        return OmniMetricHelper.registerOmniMetrics(this.metrics, nativeTaskMetricGroupRef);
    }
    // return nativeAddressOfStreamTask
    private native long setupStreamTaskBeforeInvoke(long nativeTaskRef, String StreamTaskClassName /* possible other
     parameter*/);

    private native long doRunRestoreNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter*/);

    private native long doRunInvokeNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter */);

    private native void dispatchOperatorEvent(long nativeTaskRef, String operatorId, String eventDesc);

    private native long doRunNativeTask(long nativeTaskRef, long streamTaskRef /* possible other parameter */);

    private native long createNativeTaskMetricGroup(long nativeTaskRef);

    private native long cancelTask(long nativeTaskRef);
}
