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

package com.huawei.boostkit.omniadvisor.executor;

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.configuration.BaseTestConfiguration;
import com.huawei.boostkit.omniadvisor.fetcher.Fetcher;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherType;
import com.huawei.boostkit.omniadvisor.spark.SparkFetcher;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;

import static org.mockito.Mockito.when;

public class TestOmniAdvisorRunner extends BaseTestConfiguration {
    @Test
    public void testOmniTuningRunner() {
        PropertiesConfiguration sparkConfig = Mockito.mock(PropertiesConfiguration.class);
        when(sparkConfig.getBoolean("spark.enable", false)).thenReturn(true);
        when(sparkConfig.getString("spark.workload", "default")).thenReturn("default");
        when(sparkConfig.getString("spark.eventLogs.mode")).thenReturn("log");
        when(sparkConfig.getInt("spark.timeout.seconds", 30)).thenReturn(30);
        URL resource = Thread.currentThread().getContextClassLoader().getResource("spark-events");
        when(sparkConfig.getString("spark.log.directory", "")).thenReturn(resource.getPath());
        when(sparkConfig.getInt("spark.log.maxSize.mb", 500)).thenReturn(500);
        Fetcher sparkFetcher = new SparkFetcher(sparkConfig);

        OmniAdvisorContext.getInstance().getFetcherFactory().addFetcher(FetcherType.SPARK, sparkFetcher);

        OmniTuningRunner runner = new OmniTuningRunner(0L, Long.MAX_VALUE);
        runner.run();
    }
}
