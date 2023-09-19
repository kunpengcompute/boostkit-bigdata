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

package com.huawei.boostkit.omnituning.spark;

import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.fetcher.FetcherType;
import com.huawei.boostkit.omnituning.models.AppResult;
import com.huawei.boostkit.omnituning.spark.data.SparkLogAnalyticJob;
import io.ebean.Finder;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestSparkFetcher {
    private static String testResourcePath;
    private static SparkFetcher sparkFetcher;

    @BeforeClass
    public static void setUp() {
        PropertiesConfiguration sparkConfig = Mockito.mock(PropertiesConfiguration.class);
        when(sparkConfig.getBoolean("spark.enable", false)).thenReturn(true);
        when(sparkConfig.getString("spark.workload", "default")).thenReturn("default");
        when(sparkConfig.getString("spark.eventLogs.mode")).thenReturn("log");
        when(sparkConfig.getInt("spark.timeout.seconds", 30)).thenReturn(30);
        URL resource = Thread.currentThread().getContextClassLoader().getResource("spark-events");
        testResourcePath = resource.getPath();
        when(sparkConfig.getString("spark.log.directory", "")).thenReturn(resource.getPath());
        when(sparkConfig.getInt("spark.log.maxSize.mb", 500)).thenReturn(500);
        sparkFetcher = new SparkFetcher(sparkConfig);
    }

    @Test
    public void testEnable() {
        assertTrue(sparkFetcher.isEnable());
    }

    @Test
    public void testFetcherType() {
        assertEquals(sparkFetcher.getType(), FetcherType.SPARK);
    }

    @Test
    public void testGetApplications() {
        Finder finder = Mockito.mock(Finder.class);
        when(finder.byId(any())).thenReturn(null);
        AppResult.FINDER = finder;
        List<AnalyticJob> jobs = sparkFetcher.fetchAnalyticJobs(0L, Long.MAX_VALUE);
        assertEquals(jobs.size(), 1);
    }

    @Test
    public void testAnalysis() {
        SparkLogAnalyticJob logJob = new SparkLogAnalyticJob("appId",
                testResourcePath + System.getProperty("file.separator") + "application_1516285256255_0012");
        Optional<AppResult> result = sparkFetcher.analysis(logJob);
        assertTrue(result.isPresent());
        AppResult appResult = result.get();
        assertEquals(appResult.applicationId, "application_1516285256255_0012");
        assertEquals(appResult.applicationName, "Spark shell");
        assertEquals(appResult.applicationWorkload, "default");
        assertEquals(appResult.startTime, 1516300235119L);
        assertEquals(appResult.finishTime, 1516300707938L);
        assertEquals(appResult.durationTime, 472819L);
        assertEquals(appResult.jobType, "SPARK");
        assertEquals(appResult.parameters, "{\"spark.executor.memory\":\"2G\",\"spark.executor.cores\":\"\",\"spark.executor.instances\":\"8\"}");
        assertEquals(appResult.query, "");
    }
}
