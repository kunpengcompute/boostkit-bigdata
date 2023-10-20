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

package org.apache.spark;

import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import org.apache.spark.status.api.v1.ApplicationAttemptInfo;
import org.apache.spark.status.api.v1.ApplicationEnvironmentInfo;
import org.apache.spark.status.api.v1.ApplicationInfo;
import org.apache.spark.status.api.v1.JobData;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static scala.collection.JavaConverters.asScalaBuffer;

public class TestSparkApplicationDataExtractor {
    private static final String TEST_WORK_LOAD = "default";

    private static ApplicationEnvironmentInfo environmentInfo;
    private static ApplicationAttemptInfo completeAttemptInfo;
    private static ApplicationAttemptInfo unCompleteAttemptInfo;

    private static JobData jobData;
    private static JobData failedData;
    private static JobData runningData;

    @BeforeClass
    public static void setUp() throws ParseException {
        List<Tuple2<String, String>> configs = ImmutableList.of(
                new Tuple2<>("spark.executor.memory", "1g"),
                new Tuple2<>("spark.executor.cores", "1"),
                new Tuple2<>("spark.executor.instances", "1"));
        environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(configs));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date startDate = format.parse("2020-05-01 18:00:00");
        Date endDate = format.parse("2020-05-01 18:00:01");

        completeAttemptInfo = new ApplicationAttemptInfo(Option.apply("attemptId"), startDate, endDate, endDate, 1000L, "user", true, "3.1.1");
        unCompleteAttemptInfo = new ApplicationAttemptInfo(Option.apply("attemptId"), startDate, endDate, endDate, 1000L, "user", false, "3.1.1");

        jobData = new JobData(1, "jobName", Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.SUCCEEDED, 0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, new HashMap<>());
        failedData = new JobData(1, "jobName", Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.FAILED, 0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, new HashMap<>());
        runningData = new JobData(1, "jobName", Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.RUNNING, 0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, new HashMap<>());
    }

    @Test
    public void testExtractData() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of(jobData)));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.SUCCEEDED_STATUS);
        assertEquals(result.durationTime, 1000L);
    }

    @Test
    public void testExtractDataWithUnCompleteApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(unCompleteAttemptInfo)));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of(runningData)));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test
    public void testExtractDataWithFailedApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of(failedData)));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test
    public void testExtractDataWithEmptyJob() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractDataWithEmptyApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()));
        SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of()));
    }
}
