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

package com.huawei.boostkit.omnituning.tez;

import com.google.common.annotations.VisibleForTesting;
import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.fetcher.Fetcher;
import com.huawei.boostkit.omnituning.fetcher.FetcherType;
import com.huawei.boostkit.omnituning.models.AppResult;
import com.huawei.boostkit.omnituning.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omnituning.tez.data.TezDagIdData;
import com.huawei.boostkit.omnituning.tez.utils.TezJsonUtils;
import com.huawei.boostkit.omnituning.tez.utils.TezUrlFactory;
import com.huawei.boostkit.omnituning.utils.Utils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.huawei.boostkit.omnituning.utils.Utils.loadParamsFromConf;

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
    public List<AnalyticJob> fetchAnalyticJobs(long startTimeMills, long finishedTimeMills) {
        try {
            return tezJsonUtils.getApplicationJobs(startTimeMills, finishedTimeMills);
        } catch (IOException | AuthenticationException e) {
            LOG.error("Fetch applications from timeline server failed.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<AppResult> analysis(AnalyticJob job) {
        if (!(job instanceof TezAnalyticJob)) {
            throw new OmniTuningException("TezFetcher only support TezAnalyticJob");
        }
        TezAnalyticJob tezJob = (TezAnalyticJob) job;

        List<TezDagIdData> dagIds;
        try {
            dagIds = tezJsonUtils.getDAGIds(job.getApplicationId());
        } catch (IOException e) {
            LOG.error("Get dagIds from timeline server failed.", e);
            return Optional.empty();
        }

        if (dagIds.isEmpty()) {
            LOG.info("There is no dag in application {}, skip it", job.getApplicationId());
            return Optional.empty();
        }

        // If there is more than one dag in application, only analyze the last one
        TezDagIdData tezDagId = dagIds.stream().max(TezDagIdData::compareTo).get();

        return extractAppResult(tezJob, tezDagId);
    }

    private Optional<AppResult> extractAppResult(TezAnalyticJob tezJob, TezDagIdData dagIdData) {
        LOG.info("Analyzing dag {}", dagIdData.getDagId());
        AppResult appResult = new AppResult();
        Map<String, String> jobConf;
        try {
            jobConf = tezJsonUtils.getConfigure(tezJob.getApplicationId());
            appResult.parameters = Utils.parseMapToJsonString(loadParamsFromConf(TEZ_PARAMS_CONF_FILE, jobConf));
            appResult.query = tezJsonUtils.getQueryString(dagIdData.getDagId());
        } catch (IOException e) {
            LOG.error("Analyze job failed. ", e);
            return Optional.empty();
        }

        appResult.applicationId = tezJob.getApplicationId();
        appResult.applicationName = tezJob.getApplicationName();
        appResult.applicationWorkload = workload;
        appResult.jobType = tezJob.getType().getName();

        if (dagIdData.isComplete()) {
            appResult.startTime = dagIdData.getStartTime();
            appResult.finishTime = dagIdData.getEndTime();
            appResult.executionStatus = dagIdData.isSuccess() ? AppResult.SUCCEEDED_STATUS : AppResult.FAILED_STATUS;
            appResult.durationTime = dagIdData.isSuccess() ? dagIdData.getDuration() : AppResult.FAILED_JOB_DURATION;
        } else {
            appResult.startTime = tezJob.getStartTimeMills();
            appResult.finishTime = tezJob.getFinishTimeMills();
            if (tezJob.getState() == YarnApplicationState.KILLED) {
                LOG.info("Application {} is killed, regarded as a failed task", tezJob.getApplicationId());
                appResult.executionStatus = AppResult.FAILED_STATUS;
                appResult.durationTime = AppResult.FAILED_JOB_DURATION;
            } else {
                LOG.info("Application {} using input time", tezJob.getApplicationId());
                appResult.executionStatus = AppResult.SUCCEEDED_STATUS;
                appResult.durationTime = appResult.finishTime - appResult.startTime;
            }
        }

        return Optional.of(appResult);
    }

    @VisibleForTesting
    protected void setTezJsonUtils(TezJsonUtils jsonUtils) {
        this.tezJsonUtils = jsonUtils;
    }
}
