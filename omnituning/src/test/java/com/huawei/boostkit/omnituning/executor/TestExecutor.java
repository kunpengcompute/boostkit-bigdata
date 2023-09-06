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

package com.huawei.boostkit.omnituning.executor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.huawei.boostkit.omnituning.fetcher.FetcherFactory;
import com.huawei.boostkit.omnituning.spark.data.SparkRestAnalyticJob;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestExecutor {
    @Test
    public void testExecutor() {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("omni-tuning-test-thread-%d").build();
        PropertiesConfiguration configuration = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(configuration.getBoolean("spark.enable", false)).thenReturn(false);
        Mockito.when(configuration.getBoolean("tez.enable", false)).thenReturn(false);


        FetcherFactory fetcherFactory = new FetcherFactory(configuration);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), factory);

        threadPoolExecutor.submit(new ExecutorJob(new SparkRestAnalyticJob("id"), fetcherFactory, new Object()));

        Timer timer = new Timer();
        timer.schedule(new ThreadPoolListener(timer, threadPoolExecutor), 1, 1);
    }
}
