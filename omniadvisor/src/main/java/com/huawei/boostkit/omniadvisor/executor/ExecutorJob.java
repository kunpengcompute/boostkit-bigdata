/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.executor;

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult;
import com.huawei.boostkit.omniadvisor.fetcher.Fetcher;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherFactory;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherType;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import io.ebean.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static com.huawei.boostkit.omniadvisor.utils.MathUtils.SECOND_IN_MS;

class ExecutorJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorJob.class);

    private final AnalyticJob analyticJob;
    private final FetcherFactory fetcherFactory;
    private final Object appsLock;

    public ExecutorJob(AnalyticJob analyticJob, FetcherFactory fetcherFactory, Object appsLock) {
        this.analyticJob = analyticJob;
        this.fetcherFactory = fetcherFactory;
        this.appsLock = appsLock;
    }

    @Override
    public void run() {
        OmniAdvisorContext context = OmniAdvisorContext.getInstance();
        FetcherType type = analyticJob.getType();
        String appId = analyticJob.getApplicationId();

        LOG.info("Analyzing {} {}", type.getName(), appId);

        long analysisStartTime = System.currentTimeMillis();

        Fetcher fetcher = fetcherFactory.getFetcher(type);

        final Optional<ApplicationResult> result = fetcher.analysis(analyticJob);
        if (result.isPresent()) {
            synchronized (appsLock) {
                ApplicationResult analyzeResult = result.get();
                LOG.info("Analysis get result {}", appId);
                Map<String, ApplicationResult> applicationResultMap = context.getApplicationResultMap();
                try {
                    applicationResultMap.put(analyzeResult.getApplicationId(), analyzeResult);
                } catch (Throwable e) {
                    LOG.error("Error in saving analyze result, {}", e.getMessage());
                }
            }
        } else {
            LOG.info("Analysis get empty result {}", appId);
        }

        long analysisTimeMills = System.currentTimeMillis() - analysisStartTime;

        LOG.info("Finish analysis {} {} using {}s", type, appId, analysisTimeMills / SECOND_IN_MS);
    }
}