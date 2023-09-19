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

package com.huawei.boostkit.omnituning.tez;

import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.configuration.TestConfiguration;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.fetcher.FetcherType;
import com.huawei.boostkit.omnituning.models.AppResult;
import com.huawei.boostkit.omnituning.spark.data.SparkRestAnalyticJob;
import com.huawei.boostkit.omnituning.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omnituning.tez.utils.TestTezClient;
import io.ebean.Finder;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Optional;

import static com.huawei.boostkit.omnituning.tez.utils.TestJsonUtilsFactory.getAppListJsonUtils;
import static com.huawei.boostkit.omnituning.tez.utils.TestJsonUtilsFactory.getFailedJsonUtils;
import static com.huawei.boostkit.omnituning.tez.utils.TestJsonUtilsFactory.getKilledJsonUtils;
import static com.huawei.boostkit.omnituning.tez.utils.TestJsonUtilsFactory.getSuccessJsonUtils;
import static com.huawei.boostkit.omnituning.tez.utils.TestJsonUtilsFactory.getUnFinishedJsonUtils;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.FAILED_JOB;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.KILLED_JOB;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.SUCCESS;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.SUCCESS_JOB;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.TIME_14;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.TIME_18;
import static com.huawei.boostkit.omnituning.tez.utils.TestTezContext.UNFINISHED_JOB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestTezFetcher extends TestConfiguration {
    @Test
    public void testGetApplicationFromTimeline() throws IOException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTimelineClient(TestTezClient.getTestTimelineClient());
        Finder finder = Mockito.mock(Finder.class);
        when(finder.byId(any())).thenReturn(null);
        AppResult.FINDER = finder;
        List<AnalyticJob> job = fetcher.fetchAnalyticJobs(0L, 100L);
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
        List<AnalyticJob> jobs = fetcher.fetchAnalyticJobs(0L, 100L);
        assertTrue(jobs.isEmpty());
    }

    @Test
    public void testAnalyzeWithError() {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        Optional<AppResult> result = fetcher.analysis(new TezAnalyticJob("id", "name", 0L, 1L, YarnApplicationState.FINISHED));
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
        List<AnalyticJob> tezJobs = fetcher.fetchAnalyticJobs(TIME_14, TIME_18);
        assertEquals(tezJobs.size(), 4);
    }

    @Test(expected = OmniTuningException.class)
    public void testAnalyzeJobWithErrorType() {
        SparkRestAnalyticJob sparkRestAnalyticJob = new SparkRestAnalyticJob("sparkRest");
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        Optional<AppResult> tezJobs = fetcher.analysis(sparkRestAnalyticJob);
    }

    @Test
    public void testAnalyzeJobWithSuccessJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getSuccessJsonUtils());
        Optional<AppResult> successJob = fetcher.analysis(SUCCESS_JOB);
        assertTrue(successJob.isPresent());
        assertEquals(successJob.get().applicationId, SUCCESS);
    }

    @Test
    public void testAnalyzeJobWithFailedJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getFailedJsonUtils());
        Optional<AppResult> failedJob = fetcher.analysis(FAILED_JOB);
        assertTrue(failedJob.isPresent());
    }

    @Test
    public void testAnalyzeJobWithKilledJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getKilledJsonUtils());
        Optional<AppResult> killedJob = fetcher.analysis(KILLED_JOB);
        assertTrue(killedJob.isPresent());
    }

    @Test
    public void testAnalyzeJobWithUnFinishedJob() throws MalformedURLException {
        TezFetcher fetcher = new TezFetcher(testConfiguration);
        fetcher.setTezJsonUtils(getUnFinishedJsonUtils());
        Optional<AppResult> unFinishedJob = fetcher.analysis(UNFINISHED_JOB);
        assertTrue(unFinishedJob.isPresent());
    }
}
