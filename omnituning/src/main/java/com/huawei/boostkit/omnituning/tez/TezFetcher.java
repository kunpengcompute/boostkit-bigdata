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

import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.fetcher.Fetcher;
import com.huawei.boostkit.omnituning.fetcher.FetcherType;
import com.huawei.boostkit.omnituning.models.AppResult;
import com.huawei.boostkit.omnituning.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omnituning.tez.utils.JsonUtils;
import com.huawei.boostkit.omnituning.tez.utils.TimelineUtils;
import com.huawei.boostkit.omnituning.tez.utils.UrlFactory;
import com.huawei.boostkit.omnituning.utils.Utils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.huawei.boostkit.omnituning.models.AppResult.FAILED_JOB_DURATION;
import static com.huawei.boostkit.omnituning.utils.Utils.loadParamsFromConf;

public class TezFetcher implements Fetcher {
    private static final Logger logger = LoggerFactory.getLogger(TezFetcher.class);

    private static final String TEZ_ENABLE_KEY = "tez.enable";
    private static final String TEZ_WORKLOAD_KEY = "tez.workload";
    private static final String TEZ_TIMELINE_URL_KEY = "tez.timeline.url";
    private static final String DEFAULT_WORKLOAD = "default";
    private static final String DEFAULT_TIMELINE_URL = "http://localhost:8188";

    private final boolean enable;
    private String workload;
    private JsonUtils jsonUtils;

    public TezFetcher(PropertiesConfiguration configuration) {
        this.enable = configuration.getBoolean(TEZ_ENABLE_KEY, false);
        if (enable) {
            String timelineUrl = configuration.getString(TEZ_TIMELINE_URL_KEY, DEFAULT_TIMELINE_URL);
            UrlFactory urlFactory = new UrlFactory(timelineUrl);
            this.jsonUtils = new JsonUtils(urlFactory);
            this.workload = configuration.getString(TEZ_WORKLOAD_KEY, DEFAULT_WORKLOAD);
        }
    }

    @Override
    public boolean isEnable() {
        if (enable) {
            try {
                jsonUtils.verifyTimeLineServer();
                return true;
            } catch (IOException e) {
                logger.error("Connect to timeline server failed {}, TEZ fetcher is disabled", e.getMessage());
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
            return jsonUtils.getApplicationJobs(startTimeMills, finishedTimeMills);
        } catch (IOException | AuthenticationException e) {
            logger.error("Fetch applications from timeline server failed.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<AppResult> analysis(AnalyticJob job) {
        if (!(job instanceof TezAnalyticJob)) {
            throw new OmniTuningException("TezFetcher only support TezAnalyticJob");
        }
        TezAnalyticJob tezJob = (TezAnalyticJob) job;

        // get dagIds, if datId is empty, skip it
        List<String> dagIds;
        try {
            dagIds = jsonUtils.getDAGIds(job.getApplicationId());
        } catch (IOException | AuthenticationException e) {
            logger.error("Get dagIds from timeline server failed. {}", e.getMessage());
            return Optional.empty();
        }

        if (dagIds.isEmpty()) {
            return Optional.empty();
        }

        return extractAppResult(tezJob, dagIds);
    }

    private Optional<AppResult> extractAppResult(AnalyticJob job, List<String> dagIds) {
        AppResult appResult = new AppResult();
        appResult.applicationId = tezJob.getApplicationId();
        appResult.jobType = tezJob.getType().getName();
        appResult.startTime = tezJob.getStartedTime();
        appResult.finishTIme = tezJob.getFinishedTime();
        appResult.applicationWorkload = workload;
        appResult.executionStatus = tezJob.isSuccess() ? AppResult.SUCCEEDED_STATUS : AppResult.FAILED_STATUS;
        appResult.durationTime = tezJob.isSuccess() ? (tezJob.getFinishedTime() - tezJob.getStartedTime())
                : FAILED_JOB_DURATION;
        Map<String, String> jobConf;

        try {
            jobConf = jsonUtils.getConfigure(job.getApplicationId());
        } catch (IOException | AuthenticationException e) {
            logger.error(e.getMessage());
            return Optional.empty();
        }

        String query = null;

        for (String dagId : dagIds) {
            try {
                // get first query string as query
                if (query == null) {
                    query = jsonUtils.getQueryString(dagId);
                }
            } catch (IOException | AuthenticationException e) {
                logger.error(e.getMessage());
            } finally {
                TimelineUtils.updateAuthToken();
            }
        }

        String applicationName = query == null ? "" : TimelineUtils.getSHA256String(query);
        appResult.applicationWorkload = "default";
        appResult.applicationName = applicationName;
        appResult.query = query;
        appResult.parameters = Utils.parseMapToJsonString(loadParamsFromConf("TEZParam", jobConf));

        return Optional.of(appResult);
    }
}
