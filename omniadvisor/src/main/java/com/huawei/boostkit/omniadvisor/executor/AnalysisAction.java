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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.configuration.OmniAdvisorConfigure;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.fetcher.Fetcher;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherFactory;
import com.huawei.boostkit.omniadvisor.security.HadoopSecurity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AnalysisAction implements PrivilegedAction<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisAction.class);

    private static final int WAIT_INTERVAL = 1000;

    private final HadoopSecurity hadoopSecurity;
    private final long startTimeMills;
    private final long finishTimeMills;

    private final Object appsLock;

    public AnalysisAction(HadoopSecurity hadoopSecurity,long startTimeMills, long finishTImeMills) {
        this.appsLock = new Object();
        this.hadoopSecurity = hadoopSecurity;
        this.startTimeMills = startTimeMills;
        this.finishTimeMills = finishTImeMills;
    }

    @Override
    public Void run() {
        OmniAdvisorContext context = OmniAdvisorContext.getInstance();

        FetcherFactory fetcherFactory = context.getFetcherFactory();
        OmniAdvisorConfigure omniAdvisorConfigure = context.getOmniAdvisorConfigure();

        try {
            hadoopSecurity.checkLogin();
        } catch (IOException e) {
            LOG.error("Error with hadoop kerberos login", e);
            throw new OmniAdvisorException(e);
        }

        LOG.info("Fetching analytic job list");

        List<AnalyticJob> analyticJobs = new ArrayList<>();
        for (Fetcher fetcher : fetcherFactory.getAllFetchers()) {
            LOG.info("Fetching jobs from {}", fetcher.getType().getName());
            List<AnalyticJob> fetchedJobs = fetcher.fetchAnalyticJobs(startTimeMills, finishTimeMills);
            LOG.info("Fetched {} jobs from {}", fetchedJobs.size(), fetcher.getType().getName());
            analyticJobs.addAll(fetchedJobs);
        }

        LOG.info("Fetchers get total {} Jobs", analyticJobs.size());

        if (!analyticJobs.isEmpty()) {
            ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("omni-tuning-thread-%d").build();
            int executorNum = Integer.min(analyticJobs.size(), omniAdvisorConfigure.getThreadCount());
            int queueSize = Integer.max(analyticJobs.size(), executorNum);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(executorNum, executorNum, 0L,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize), factory);
            for (AnalyticJob analyticJob : analyticJobs) {
                synchronized (appsLock) {
                    threadPoolExecutor.submit(new ExecutorJob(analyticJob, fetcherFactory, appsLock));
                }
            }
            Timer timer = new Timer();
            timer.schedule(new ThreadPoolListener(timer, threadPoolExecutor), WAIT_INTERVAL, WAIT_INTERVAL);
        }
        return null;
    }
}
