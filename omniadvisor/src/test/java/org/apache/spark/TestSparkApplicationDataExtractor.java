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
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.status.api.v1.ApplicationAttemptInfo;
import org.apache.spark.status.api.v1.ApplicationEnvironmentInfo;
import org.apache.spark.status.api.v1.ApplicationInfo;
import org.apache.spark.status.api.v1.JobData;
import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashSet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static scala.collection.JavaConverters.asScalaBuffer;

public class TestSparkApplicationDataExtractor {
    private static final String TEST_WORK_LOAD = "default";
    private static String testResourcePath;
    private static List<Tuple2<String, String>> clientSparkProperties;
    private static List<Tuple2<String, String>> clusterSparkProperties;
    private static ApplicationAttemptInfo completeAttemptInfo;
    private static ApplicationAttemptInfo unCompleteAttemptInfo;
    private static List<JobData> jobsList;
    private static JobData failedData;
    private static JobData runningData;

    @BeforeClass
    public static void setUp() throws ParseException {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("application-info");
        assert resource != null;
        testResourcePath = resource.getPath();

        clientSparkProperties = ImmutableList.of(
                new Tuple2<>("spark.executor.memory", "1g"),
                new Tuple2<>("spark.executor.cores", "1"),
                new Tuple2<>("spark.executor.instances", "1"),
                new Tuple2<>("spark.submit.deployMode", "client"));

        clusterSparkProperties = ImmutableList.of(
                new Tuple2<>("spark.executor.memory", "1g"),
                new Tuple2<>("spark.executor.cores", "1"),
                new Tuple2<>("spark.executor.instances", "1"),
                new Tuple2<>("spark.submit.deployMode", "cluster"));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date appStartDate = format.parse("2023-12-21 15:00:00");
        Date appFinishDate = format.parse("2023-12-21 15:15:00");

        Date firstJobSubmissionTime = format.parse("2023-12-21 15:00:00");
        Date firstJobCompletionTime = format.parse("2023-12-21 15:10:00");
        Date secondJobSubmissionTime = format.parse("2023-12-21 15:05:00");
        Date secondJobCompletionTime = format.parse("2023-12-21 15:15:00");

        completeAttemptInfo = new ApplicationAttemptInfo(Option.apply("attemptId"), appStartDate, appFinishDate, appFinishDate, 1000L, "user", true, "3.1.1");
        unCompleteAttemptInfo = new ApplicationAttemptInfo(Option.apply("attemptId"), appStartDate, appFinishDate, appFinishDate, 1000L, "user", false, "3.1.1");

        jobsList = new ArrayList<>();
        jobsList.add(new JobData(1, "jobName1", Option.empty(), Option.apply(firstJobSubmissionTime), Option.apply(firstJobCompletionTime), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.SUCCEEDED, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new HashMap<>()));
        jobsList.add(new JobData(2, "jobName2", Option.empty(), Option.apply(secondJobSubmissionTime), Option.apply(secondJobCompletionTime), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.SUCCEEDED, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new HashMap<>()));
        failedData = new JobData(1, "jobName", Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.FAILED, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new HashMap<>());
        runningData = new JobData(1, "jobName", Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()), Option.empty(), JobExecutionStatus.RUNNING, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new HashMap<>());
    }

    @Test
    public void testExtractDataWithSparkSqlApp() throws IOException, URISyntaxException {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        String sparkSqlCmd = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSqlCmd").toUri())));
        String sparkSqlExtractedCmd = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSqlExtractedCmd").toUri())));
        String sqlQ10 = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sql_q10").toUri())));
        String sqlQ11 = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sql_q11").toUri())));
        String extractedSql = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "extractedSql").toUri())));

        List<Tuple2<String, String>> systemProperties = ImmutableList.of(new Tuple2<>("sun.java.command", sparkSqlCmd));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(clientSparkProperties));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(systemProperties));

        List<SQLExecutionUIData> sqlExecutionList = new ArrayList<>();
        sqlExecutionList.add(new SQLExecutionUIData(1, sqlQ10, "", "", asScalaBuffer(ImmutableList.of()), 0, Option.apply(new Date()), new HashMap<>(), new HashSet<>(), new HashMap<>()));
        sqlExecutionList.add(new SQLExecutionUIData(2, sqlQ11, "", "", asScalaBuffer(ImmutableList.of()), 0, Option.apply(new Date()), new HashMap<>(), new HashSet<>(), new HashMap<>()));

        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(jobsList), asScalaBuffer(sqlExecutionList));
        assertEquals(result.applicationId, "id");
        assertEquals(result.durationTime, 15 * 60 * 1000L);
        assertEquals(result.submit_method, "spark-sql");
        assertEquals(result.deploy_mode, "client");
        assertEquals(result.submit_cmd, sparkSqlExtractedCmd);
        assertEquals(result.executionStatus, AppResult.SUCCEEDED_STATUS);
        assertEquals(result.query.replaceAll(";\\s*\n", ";"), extractedSql.replaceAll(";\\s*\n", ";"));
    }

    @Test
    public void testExtractDataWithSubmitClientApp() throws IOException {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        String sparkSubmitCmdWithClientMode = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSubmitCmdWithClientMode").toUri())));
        String sparkSubmitExtractedCmdWithClientMode = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSubmitExtractedCmdWithClientMode").toUri())));

        List<Tuple2<String, String>> systemProperties = ImmutableList.of(new Tuple2<>("sun.java.command", sparkSubmitCmdWithClientMode));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(clientSparkProperties));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(systemProperties));

        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(jobsList), asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.durationTime, 15 * 60 * 1000L);
        assertEquals(result.submit_method, "spark-submit");
        assertEquals(result.deploy_mode, "client");
        assertEquals(result.submit_cmd, sparkSubmitExtractedCmdWithClientMode);
        assertEquals(result.executionStatus, AppResult.SUCCEEDED_STATUS);
    }

    @Test
    public void testExtractDataWithSubmitClusterApp() throws IOException {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        String sparkSubmitCmdWithClusterMode = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSubmitCmdWithClusterMode").toUri())));
        String sparkSubmitExtractedCmdWithClusterMode = new String(Files.readAllBytes(Paths.get(new Path("file:" + testResourcePath + System.getProperty("file.separator") + "sparkSubmitExtractedCmdWithClusterMode").toUri())));

        List<Tuple2<String, String>> systemProperties = ImmutableList.of(new Tuple2<>("sun.java.command", sparkSubmitCmdWithClusterMode));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(clusterSparkProperties));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(systemProperties));

        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(jobsList), asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.durationTime, 15 * 60 * 1000L);
        assertEquals(result.submit_method, "spark-submit");
        assertEquals(result.deploy_mode, "cluster");
        assertEquals(result.submit_cmd, sparkSubmitExtractedCmdWithClusterMode);
        assertEquals(result.executionStatus, AppResult.SUCCEEDED_STATUS);
    }

    @Test
    public void testExtractDataWithUnCompleteApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(unCompleteAttemptInfo)));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of(runningData)), asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test
    public void testExtractDataWithFailedApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of(failedData)), asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test
    public void testExtractDataWithEmptyJob() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of(completeAttemptInfo)));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        AppResult result = SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of()), asScalaBuffer(ImmutableList.of()));
        assertEquals(result.applicationId, "id");
        assertEquals(result.executionStatus, AppResult.FAILED_STATUS);
        assertEquals(result.durationTime, AppResult.FAILED_JOB_DURATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractDataWithEmptyApplication() {
        ApplicationInfo applicationInfo = new ApplicationInfo("id", "name", Option.empty(), Option.empty(), Option.empty(), Option.empty(), asScalaBuffer(ImmutableList.of()));
        ApplicationEnvironmentInfo environmentInfo = Mockito.mock(ApplicationEnvironmentInfo.class);
        when(environmentInfo.sparkProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        when(environmentInfo.systemProperties()).thenReturn(asScalaBuffer(ImmutableList.of()));
        SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(applicationInfo, TEST_WORK_LOAD, environmentInfo, asScalaBuffer(ImmutableList.of()), asScalaBuffer(ImmutableList.of()));
    }
}
