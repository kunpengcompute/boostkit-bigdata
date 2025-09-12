package com.huawei.omniruntime.flink.runtime.taskexecutor;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JobInformationPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.StreamConfigPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskInformationPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.TaskDeploymentDescriptorPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.OperatorPOJO;
import com.huawei.omniruntime.flink.runtime.shuffle.OmniShuffleEnvironment;
import com.huawei.omniruntime.flink.runtime.taskmanager.OmniTask;

import com.huawei.omniruntime.flink.streaming.api.graph.JobType;
import com.huawei.omniruntime.flink.utils.UdfUtil;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TaskExecutorBlobService;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.JobTable;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.concurrent.FutureUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

/**
 * OmniTaskExecutor
 *
 * @since 2025-04-27
 */
public class OmniTaskExecutor extends TaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTaskExecutor.class);

    private long nativeTaskExecutorReference;

    private OmniShuffleEnvironment omniShuffleEnvironment;
    private OmniTaskManagerServices omniTaskManagerServices;

    public OmniTaskExecutor(RpcService rpcService,
                            TaskManagerConfiguration taskManagerConfiguration,
                            HighAvailabilityServices haServices,
                            TaskManagerServices taskExecutorServices,
                            ExternalResourceInfoProvider externalResourceInfoProvider,
                            HeartbeatServices heartbeatServices,
                            TaskManagerMetricGroup taskManagerMetricGroup,
                            @Nullable String metricQueryServiceAddress,
                            TaskExecutorBlobService taskExecutorBlobService,
                            FatalErrorHandler fatalErrorHandler,
                            TaskExecutorPartitionTracker partitionTracker,
                            OmniTaskManagerServices omniTaskManagerServices) {
        super(rpcService, taskManagerConfiguration, haServices, taskExecutorServices, externalResourceInfoProvider,
                heartbeatServices, taskManagerMetricGroup, metricQueryServiceAddress, taskExecutorBlobService,
                fatalErrorHandler, partitionTracker);

        String taskExecutorConfig = convertTaskExecutorToJson();
        log.info("TaskExecutorConfig: " + taskExecutorConfig);
        nativeTaskExecutorReference = createNativeTaskExecutor(taskExecutorConfig,
                omniTaskManagerServices.getNativeOmniTaskManagerServicesAddress());
        log.info("nativeTaskExecutorReference: " + nativeTaskExecutorReference);
    }

    private static UserCodeClassLoader createUserCodeClassloader(
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            Collection<PermanentBlobKey> requiredJarFiles,
            Collection<URL> requiredClasspaths) throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

        LOG.debug(
                "Getting user code class loader for task at library cache manager took {} milliseconds",
                System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

    private String convertTaskExecutorToJson() {
        try {
            Class<?> parentClass = this.getClass().getSuperclass();
            Field taskConfiguration = parentClass.getDeclaredField("taskManagerConfiguration");
            taskConfiguration.setAccessible(true);
            checkState(taskConfiguration.get(this) instanceof TaskManagerConfiguration);
            TaskManagerConfiguration configuration = (TaskManagerConfiguration) taskConfiguration.get(this);
            JSONObject taskExecutorJson = new JSONObject(configuration);
            return taskExecutorJson.toString();
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TaskParam {
        private TaskDeploymentDescriptor tdd;
        private JobID jobId;
        private TaskInformation taskInformation;
        private JobInformation jobInformation;

        public TaskParam(TaskDeploymentDescriptor tdd, JobID jobId,
            TaskInformation taskInformation, JobInformation jobInformation) {
            this.tdd = tdd;
            this.jobId = jobId;
            this.taskInformation = taskInformation;
            this.jobInformation = jobInformation;
        }
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {
        try {
            final JobID jobId = tdd.getJobId();
            final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

            final JobTable.Connection jobManagerConnection = getJobManagerConnection(tdd, jobMasterId, jobId);

            // re-integrate offloaded data:
            try {
                tdd.loadBigData(taskExecutorBlobService.getPermanentBlobService());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
            }

            // deserialize the pre-serialized information
            final JobInformation jobInformation;
            final TaskInformation taskInformation;
            try {
                jobInformation =
                        tdd.getSerializedJobInformation()
                                .deserializeValue(getClass().getClassLoader());
                taskInformation =
                        tdd.getSerializedTaskInformation()
                                .deserializeValue(getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not deserialize the job or task information.", e);
            }

            LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
                jobManagerConnection.getClassLoaderHandle();

            OmniTask task = getTask(new TaskParam(tdd, jobId, taskInformation, jobInformation),
                jobManagerConnection, executionAttemptID, classLoaderHandle);

            log.info(
                    "Received task {} ({}), deploy into slot with allocation id {}.",
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    tdd.getExecutionAttemptId(),
                    tdd.getAllocationId());

            //OmniStream Extension point

            // if omnitask
            createOmniTaskIfUseOmni(tdd, taskInformation, jobInformation, classLoaderHandle, task);

            //
            boolean taskAdded;

            try {
                taskAdded = taskSlotTable.addTask(task);
            } catch (SlotNotFoundException | SlotNotActiveException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            if (taskAdded) {
                task.startTaskThread();

                setupResultPartitionBookkeeping(
                        tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                final String message =
                        "TaskManager already contains a task for id " + task.getExecutionId() + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }
        } catch (TaskSubmissionException e) {
            return FutureUtils.completedExceptionally(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createOmniTaskIfUseOmni(TaskDeploymentDescriptor tdd, TaskInformation taskInformation,
        JobInformation jobInformation, LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
        OmniTask task) throws Exception {
        boolean useomniFlag = taskInformation.getTaskConfiguration().getBoolean("useomni", false);
        int jobType = taskInformation.getTaskConfiguration().getInteger("jobType", 0);
        log.info("Task name is {} and useOmniFlag is {} ", taskInformation.getTaskName(), useomniFlag);
        if (useomniFlag) {
            // stream config pojo
            Collection<PermanentBlobKey> requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
            Collection<URL> requiredClasspaths = jobInformation.getRequiredClasspathURLs();
            UserCodeClassLoader codeClassLoader = createUserCodeClassloader(classLoaderHandle,
                requiredJarFiles, requiredClasspaths);
            Configuration conf = taskInformation.getTaskConfiguration();
            StreamConfig streamConfig = new StreamConfig(conf);
            StreamConfigPOJO streamConfigPOJO = new StreamConfigPOJO(streamConfig, codeClassLoader.asClassLoader());
            LOG.info("StreamConfigPOJO is {}", streamConfigPOJO);
            LOG.info("StreamConfigPOJO JSON is {}", JsonHelper.toJson(streamConfigPOJO));

            // task information pojo
            TaskInformationPOJO taskInformationPOJO = new TaskInformationPOJO(taskInformation,
                codeClassLoader.asClassLoader(), task.getTaskInfo().getIndexOfThisSubtask());
            LOG.info("TaskInformationPOJO is {}", taskInformationPOJO);
            LOG.info("TaskInformationPOJO JSON is {}", JsonHelper.toJson(taskInformationPOJO));

            // job information POJO
            JobInformationPOJO jobInformationPOJO = new JobInformationPOJO(
                jobInformation, codeClassLoader.asClassLoader());
            LOG.info("JobInformationPOJO is {}", jobInformationPOJO);
            LOG.info("JobInformationPOJO JSON is {}", JsonHelper.toJson(jobInformationPOJO));

            // tdd pojo
            TaskDeploymentDescriptorPOJO tddPojo = new TaskDeploymentDescriptorPOJO(tdd);
            LOG.info("TaskDeploymentDescriptorPOJO is {}", tddPojo);
            LOG.info("TaskDeploymentDescriptorPOJO JSON is {}", JsonHelper.toJson(tddPojo));

            // update udf info
            for (StreamConfigPOJO config : taskInformationPOJO.getChainedConfig()) {
                OperatorPOJO operatorDescriptionPOJO = config.getOperatorDescription();
                String description = operatorDescriptionPOJO.getDescription();
                JobID jobID = jobInformation.getJobId();
                String jarPath = UdfUtil.getJobJarPath(jobID);
                description = JsonHelper.updateJsonString(description, jarPath);
                operatorDescriptionPOJO.setDescription(description);
            }
            {
                OperatorPOJO pojo = taskInformationPOJO.getStreamConfig().getOperatorDescription();
                String description = pojo.getDescription();
                JobID jobID = jobInformation.getJobId();
                String jarPath = UdfUtil.getJobJarPath(jobID);
                description = JsonHelper.updateJsonString(description, jarPath);
                pojo.setDescription(description);
            }

            long nativeTaskAddress = submitTaskNative(this.nativeTaskExecutorReference, JsonHelper.toJson(jobInformationPOJO),
                    JsonHelper.toJson(taskInformationPOJO), JsonHelper.toJson(tddPojo));

            LOG.info("task {} native address is {} ", task.getExecutionId(), nativeTaskAddress);
            ((OmniTask) task).bindNativeTask(nativeTaskAddress);
            ((OmniTask) task).setJobType(JobType.fromValue(jobType));
        }

    }

    private OmniTask getTask(TaskParam taskParam, JobTable.Connection jobManagerConnection,
                             ExecutionAttemptID executionAttemptID,
                             LibraryCacheManager.ClassLoaderHandle classLoaderHandle) throws TaskSubmissionException {
        if (!taskParam.jobId.equals(taskParam.jobInformation.getJobId())) {
            throw new TaskSubmissionException(
                    "Inconsistent job ID information inside TaskDeploymentDescriptor ("
                            + taskParam.tdd.getJobId() + " vs. " + taskParam.jobInformation.getJobId() + ")");
        }

        TaskManagerJobMetricGroup jobGroup = taskManagerMetricGroup.addJob(
                taskParam.jobInformation.getJobId(), taskParam.jobInformation.getJobName());

        TaskMetricGroup taskMetricGroup =
                jobGroup.addTask(taskParam.tdd.getExecutionAttemptId(), taskParam.taskInformation.getTaskName());

        InputSplitProvider inputSplitProvider = new RpcInputSplitProvider(jobManagerConnection.getJobManagerGateway(),
                taskParam.taskInformation.getJobVertexId(), taskParam.tdd.getExecutionAttemptId(),
                taskManagerConfiguration.getRpcTimeout());

        final TaskOperatorEventGateway taskOperatorEventGateway =
                new RpcTaskOperatorEventGateway(jobManagerConnection.getJobManagerGateway(),
                        executionAttemptID, (t) -> runAsync(() -> failTask(executionAttemptID, t)));

        TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
        CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
        GlobalAggregateManager aggregateManager = jobManagerConnection.getGlobalAggregateManager();
        PartitionProducerStateChecker partitionStateChecker = jobManagerConnection.getPartitionStateChecker();

        final TaskStateManager taskStateManager = getTaskStateManager(taskParam, jobGroup, checkpointResponder);

        MemoryManager memoryManager;
        try {
            memoryManager = taskSlotTable.getTaskMemoryManager(taskParam.tdd.getAllocationId());
        } catch (SlotNotFoundException e) {
            throw new TaskSubmissionException("Could not submit task.", e);
        }

        OmniTask task =
                new OmniTask(taskParam.jobInformation, taskParam.taskInformation, taskParam.tdd.getExecutionAttemptId(),
                        taskParam.tdd.getAllocationId(), taskParam.tdd.getProducedPartitions(), taskParam.tdd.getInputGates(),
                        memoryManager, taskExecutorServices.getIOManager(), taskExecutorServices.getShuffleEnvironment(),
                        taskExecutorServices.getKvStateService(), taskExecutorServices.getBroadcastVariableManager(),
                        taskExecutorServices.getTaskEventDispatcher(), externalResourceInfoProvider, taskStateManager,
                        taskManagerActions, inputSplitProvider, checkpointResponder, taskOperatorEventGateway,
                        aggregateManager, classLoaderHandle, fileCache, taskManagerConfiguration, taskMetricGroup,
                        partitionStateChecker, getRpcService().getScheduledExecutor());

        taskMetricGroup.gauge(MetricNames.IS_BACK_PRESSURED, task::isBackPressured);
        return task;
    }


    private TaskStateManager getTaskStateManager(TaskParam taskParam, TaskManagerJobMetricGroup jobGroup,
        CheckpointResponder checkpointResponder) throws TaskSubmissionException {
        final TaskLocalStateStore localStateStore =
            localStateStoresManager.localStateStoreForSubtask(
                taskParam.jobId,
                taskParam.tdd.getAllocationId(),
                taskParam.taskInformation.getJobVertexId(),
                taskParam.tdd.getSubtaskIndex(),
                taskManagerConfiguration.getConfiguration(),
                taskParam.jobInformation.getJobConfiguration());

        final StateChangelogStorage<?> changelogStorage;
        try {
            changelogStorage =
                changelogStoragesManager.stateChangelogStorageForJob(
                    taskParam.jobId,
                    taskManagerConfiguration.getConfiguration(),
                    jobGroup,
                    localStateStore.getLocalRecoveryConfig());
        } catch (IOException e) {
            throw new TaskSubmissionException(e);
        }

        final JobManagerTaskRestore taskRestore = taskParam.tdd.getTaskRestore();

        return new TaskStateManagerImpl(
                taskParam.jobId,
                taskParam.tdd.getExecutionAttemptId(),
                localStateStore,
                changelogStorage,
                changelogStoragesManager,
                taskRestore,
                checkpointResponder);
    }

    private JobTable.Connection getJobManagerConnection(TaskDeploymentDescriptor tdd, JobMasterId jobMasterId,
        JobID jobId) throws TaskSubmissionException {
        final JobTable.Connection jobManagerConnection =
            jobTable.getConnection(jobId)
                .orElseThrow(
                    () -> {
                        final String message =
                            "Could not submit task because there is no JobManager "
                                + "associated for the job "
                                + jobId
                                + '.';

                        log.debug(message);
                        return new TaskSubmissionException(message);
                    });

        if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
            final String message =
                "Rejecting the task submission because the job manager leader id "
                    + jobMasterId
                    + " does not match the expected job manager leader id "
                    + jobManagerConnection.getJobMasterId()
                    + '.';

            log.debug(message);
            throw new TaskSubmissionException(message);
        }

        if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
            final String message =
                "No task slot allocated for job ID "
                    + jobId
                    + " and allocation ID "
                    + tdd.getAllocationId()
                    + '.';
            log.debug(message);
            throw new TaskSubmissionException(message);
        }
        return jobManagerConnection;
    }

    private native long createNativeTaskExecutor(
            String taskExecutorConfiguration,
            long nativeTaskManagerServiceAddress);

    // return nativeTaskAddress
    private native long submitTaskNative(
            long nativeTaskExecutorReference,
            String jobJson,
            String taskJson,
            String tddJson);
}
