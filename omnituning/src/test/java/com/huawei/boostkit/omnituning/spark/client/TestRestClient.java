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

package com.huawei.boostkit.omnituning.spark.client;

import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.spark.data.SparkRestAnalyticJob;
import org.apache.spark.SparkConf;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.ProcessingException;

public class TestRestClient {
    private static SparkRestClient restClient;

    @BeforeClass
    public static void setUp() {
        restClient = new SparkRestClient("history-url", 1, new SparkConf(), "default");
    }

    @Test(expected = OmniTuningException.class)
    public void testGetApplications() {
        restClient.fetchAnalyticJobs(0L, 100L);
    }

    @Test(expected = ProcessingException.class)
    public void testAnalysis() {
        SparkRestAnalyticJob restJob = new SparkRestAnalyticJob("appId");
        restClient.fetchAnalyticResult(restJob);
    }
}
