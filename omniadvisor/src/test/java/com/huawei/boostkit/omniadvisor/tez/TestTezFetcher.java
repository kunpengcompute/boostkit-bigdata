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

package com.huawei.boostkit.omniadvisor.tez;

import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.configuration.BaseTestConfiguration;
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherType;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import com.huawei.boostkit.omniadvisor.spark.data.SparkRestAnalyticJob;
import com.huawei.boostkit.omniadvisor.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omniadvisor.tez.utils.TestTezClient;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Optional;

import static com.huawei.boostkit.omniadvisor.tez.utils.TestJsonUtilsFactory.getAppListJsonUtils;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestJsonUtilsFactory.getFailedJsonUtils;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestJsonUtilsFactory.getKilledJsonUtils;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestJsonUtilsFactory.getSuccessJsonUtils;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestJsonUtilsFactory.getUnFinishedJsonUtils;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.FAILED_JOB;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.KILLED_JOB;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.SUCCESS;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.SUCCESS_JOB;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TIME_14;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TIME_18;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.UNFINISHED_JOB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTezFetcher extends BaseTestConfiguration {
    @Test
    public void testGetApplicationFromTimeline() throws IOException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTimelineClient(TestTezClient.getTestTimelineClient());
        List<AnalyticJob> job = fetcher.fetchAnalyticJobs(0L, 100L, null);
        assertEquals(job.size(), 1);
    }

    @Test
    public void testGetType() {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        assertEquals(fetcher.getType(), FetcherType.TEZ);
    }

    @Test
    public void tesGetApplicationsWithError() {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        List<AnalyticJob> jobs = fetcher.fetchAnalyticJobs(0L, 100L, null);
        assertTrue(jobs.isEmpty());
    }

    @Test
    public void testAnalyzeWithError() {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        Optional<ApplicationResult> result = fetcher.analysis(new TezAnalyticJob("id", "name", 0L, 1L, YarnApplicationState.FINISHED));
        assertFalse(result.isPresent());
    }

    @Test
    public void testEnable() {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        assertFalse(fetcher.isEnable());
    }

    @Test
    public void testAnalyze() throws IOException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTimelineClient(TestTezClient.getTestTimelineClient());
        AnalyticJob testJob = new TezAnalyticJob("test", "test", 0, 100, YarnApplicationState.FINISHED);
        fetcher.analysis(testJob);
    }

    @Test
    public void testGetApplications() throws AuthenticationException, IOException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getAppListJsonUtils());
        List<AnalyticJob> tezJobs = fetcher.fetchAnalyticJobs(TIME_14, TIME_18, null);
        assertEquals(tezJobs.size(), 4);
    }

    @Test(expected = OmniAdvisorException.class)
    public void testAnalyzeJobWithErrorType() {
        SparkRestAnalyticJob sparkRestAnalyticJob = new SparkRestAnalyticJob("sparkRest");
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.analysis(sparkRestAnalyticJob);
    }

    @Test
    public void testAnalyzeJobWithSuccessJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getSuccessJsonUtils());
        Optional<ApplicationResult> successJob = fetcher.analysis(SUCCESS_JOB);
        assertTrue(successJob.isPresent());
        assertEquals(successJob.get().getApplicationId(), SUCCESS);
    }

    @Test
    public void testAnalyzeJobWithFailedJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getFailedJsonUtils());
        Optional<ApplicationResult> failedJob = fetcher.analysis(FAILED_JOB);
        assertTrue(failedJob.isPresent());
    }

    @Test
    public void testAnalyzeJobWithKilledJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getKilledJsonUtils());
        Optional<ApplicationResult> killedJob = fetcher.analysis(KILLED_JOB);
        assertTrue(killedJob.isPresent());
    }

    @Test
    public void testAnalyzeJobWithUnFinishedJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getUnFinishedJsonUtils());
        Optional<ApplicationResult> unFinishedJob = fetcher.analysis(UNFINISHED_JOB);
        assertTrue(unFinishedJob.isPresent());
    }
}
