/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.tez;

import com.google.common.annotations.VisibleForTesting;
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.fetcher.Fetcher;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherType;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import com.huawei.boostkit.omniadvisor.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omniadvisor.tez.data.TezDagIdData;
import com.huawei.boostkit.omniadvisor.tez.utils.TezJsonUtils;
import com.huawei.boostkit.omniadvisor.tez.utils.TezUrlFactory;
import com.huawei.boostkit.omniadvisor.tez.utils.TimelineClient;
import com.huawei.boostkit.omniadvisor.utils.Utils;
import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.huawei.boostkit.omniadvisor.utils.Utils.loadParamsFromConf;

public class TezFetcher implements Fetcher {
    private static final Logger LOG = LoggerFactory.getLogger(TezFetcher.class);

    private static final String TEZ_ENABLE_KEY = "tez.enable";
    private static final String TEZ_WORKLOAD_KEY = "tez.workload";
    private static final String TEZ_TIMELINE_URL_KEY = "tez.timeline.url";
    private static final String TEZ_TIMELINE_TIMEOUT_KEY = "tez.timeline.timeout.ms";
    private static final String DEFAULT_WORKLOAD = "default";
    private static final String DEFAULT_TIMELINE_URL = "http://localhost:8188";
    private static final String HTTPS_PREFIX = "https://";
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 6000;
    private static final String TEZ_PARAMS_CONF_FILE = "TezParams";

    private final boolean enable;
    private String workload;
    private TezJsonUtils tezJsonUtils;

    public TezFetcher(PropertiesConfiguration configuration) {
        this.enable = configuration.getBoolean(TEZ_ENABLE_KEY, false);
        if (enable) {
            String timelineUrl = configuration.getString(TEZ_TIMELINE_URL_KEY, DEFAULT_TIMELINE_URL);
            TezUrlFactory tezUrlFactory = new TezUrlFactory(timelineUrl);
            this.workload = configuration.getString(TEZ_WORKLOAD_KEY, DEFAULT_WORKLOAD);
            int timeout = configuration.getInt(TEZ_TIMELINE_TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT_MS);
            boolean useHttps = timelineUrl.startsWith(HTTPS_PREFIX);
            this.tezJsonUtils = new TezJsonUtils(tezUrlFactory, useHttps, timeout);
        }
    }

    @Override
    public boolean isEnable() {
        if (enable) {
            try {
                tezJsonUtils.verifyTimeLineServer();
                return true;
            } catch (IOException e) {
                LOG.error("Connect to timeline server failed {}, TEZ fetcher is disabled", e.getMessage());
                return false;
            }
        }
        return false;
    }

    @Override
    public FetcherType getType() {
        return FetcherType.TEZ;
    }

    @Override
    public List<AnalyticJob> fetchAnalyticJobs(long startTimeMills, long finishedTimeMills, List<String> analyzedList) {
        try {
            return tezJsonUtils.getApplicationJobs(startTimeMills, finishedTimeMills, analyzedList);
        } catch (IOException | AuthenticationException | ClientHandlerException e) {
            LOG.error("Fetch applications from timeline server failed.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<ApplicationResult> analysis(AnalyticJob job) {
        if (!(job instanceof TezAnalyticJob)) {
            throw new OmniAdvisorException("TezFetcher only support TezAnalyticJob");
        }
        TezAnalyticJob tezJob = (TezAnalyticJob) job;

        List<TezDagIdData> dagIds;
        try {
            dagIds = tezJsonUtils.getDAGIds(job.getApplicationId());
        } catch (IOException | ClientHandlerException e) {
            LOG.error("Get dagIds from timeline server failed.", e);
            return Optional.empty();
        }

        if (dagIds.isEmpty()) {
            LOG.info("There is no dag in application {}, skip it", job.getApplicationId());
            return Optional.empty();
        }

        return extractAppResult(tezJob, dagIds);
    }

    private Optional<ApplicationResult> extractAppResult(TezAnalyticJob tezJob, List<TezDagIdData> dagIds) {
        LOG.info("Analyzing tez application {}", tezJob.getApplicationId());
        ApplicationResult appResult = new ApplicationResult();
        Map<String, String> jobConf;
        try {
            jobConf = tezJsonUtils.getConfigure(tezJob.getApplicationId());
            appResult.setParameters(Utils.parseMapToJsonString(loadParamsFromConf(TEZ_PARAMS_CONF_FILE, jobConf)));
            appResult.setQuery(tezJsonUtils.getQueryString(dagIds));
        } catch (IOException e) {
            LOG.error("Analyze job failed. ", e);
            return Optional.empty();
        }

        appResult.setApplicationId(tezJob.getApplicationId());
        appResult.setApplicationName(tezJob.getApplicationName());
        appResult.setApplicationWorkload(workload);
        appResult.setJobType(tezJob.getType().getName());

        if (isCompleteForDagIds(dagIds)) {
            OptionalLong minStartTime = dagIds.stream().mapToLong(TezDagIdData::getStartTime).min();
            OptionalLong maxEndTime = dagIds.stream().mapToLong(TezDagIdData::getEndTime).max();
            appResult.setStartTime(isValidTime(minStartTime, maxEndTime) ? minStartTime.getAsLong() : tezJob.getStartTimeMills());
            appResult.setFinishTime(isValidTime(minStartTime, maxEndTime) ? maxEndTime.getAsLong() : tezJob.getFinishTimeMills());
            appResult.setExecutionStatus(isSuccessfulForDagIds(dagIds) ? AppResult.SUCCEEDED_STATUS : AppResult.FAILED_STATUS);
            appResult.setDurationTime(isSuccessfulForDagIds(dagIds) && isValidTime(minStartTime, maxEndTime) ? (appResult.getFinishTime() - appResult.getStartTime()) : AppResult.FAILED_JOB_DURATION);
        } else {
            appResult.setStartTime(tezJob.getStartTimeMills());
            appResult.setFinishTime(tezJob.getFinishTimeMills());
            if (tezJob.getState() == YarnApplicationState.KILLED) {
                LOG.info("Application {} is killed, regarded as a failed task", tezJob.getApplicationId());
                appResult.setExecutionStatus(AppResult.FAILED_STATUS);
                appResult.setDurationTime(AppResult.FAILED_JOB_DURATION);
            } else {
                LOG.info("Application {} using input time", tezJob.getApplicationId());
                appResult.setExecutionStatus(AppResult.SUCCEEDED_STATUS);
                appResult.setDurationTime(appResult.getFinishTime() - appResult.getStartTime());
            }
        }

        return Optional.of(appResult);
    }

    private boolean isCompleteForDagIds(List<TezDagIdData> dagIds) {
        return dagIds.stream().allMatch(TezDagIdData::isComplete);
    }

    private boolean isSuccessfulForDagIds(List<TezDagIdData> dagIds) {
        return dagIds.stream().allMatch(TezDagIdData::isSuccess);
    }

    private boolean isValidTime(OptionalLong startTime, OptionalLong endTime) {
        return (startTime.isPresent() && endTime.isPresent() && (endTime.getAsLong() - startTime.getAsLong() > 0));
    }

    @VisibleForTesting
    protected void setTezJsonUtils(TezJsonUtils jsonUtils) {
        this.tezJsonUtils = jsonUtils;
    }

    @VisibleForTesting
    protected void setTimelineClient(TimelineClient timelineClient) {
        this.tezJsonUtils.setTimelineClient(timelineClient);
    }
}
