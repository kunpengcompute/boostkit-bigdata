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

package com.huawei.boostkit.omnituning.fetcher;

import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.spark.SparkFetcher;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestFetcherFactory {
    @Test
    public void testFetcherFactory() {
        PropertiesConfiguration config = Mockito.mock(PropertiesConfiguration.class);

        when(config.getBoolean("spark.enable", false)).thenReturn(true);
        when(config.getBoolean("tez.enable", false)).thenReturn(false);

        FetcherFactory fetcherFactory = new FetcherFactory(config);
        assertEquals(fetcherFactory.getAllFetchers().size(), 1);
    }

    @Test
    public void testFetcherFactoryWithEmptyFetcher() {
        PropertiesConfiguration config = Mockito.mock(PropertiesConfiguration.class);

        when(config.getBoolean("spark.enable", false)).thenReturn(false);
        when(config.getBoolean("tez.enable", false)).thenReturn(false);

        FetcherFactory fetcherFactory = new FetcherFactory(config);
        assertEquals(fetcherFactory.getAllFetchers().size(), 0);
    }

    @Test
    public void testGetFetcher() {
        PropertiesConfiguration config = Mockito.mock(PropertiesConfiguration.class);

        when(config.getBoolean("spark.enable", false)).thenReturn(true);
        when(config.getBoolean("tez.enable", false)).thenReturn(false);

        FetcherFactory fetcherFactory = new FetcherFactory(config);
        assertEquals(fetcherFactory.getFetcher(FetcherType.SPARK).getClass(), SparkFetcher.class);
    }

    @Test(expected = OmniTuningException.class)
    public void testGetUnknownFetcher() {
        PropertiesConfiguration config = Mockito.mock(PropertiesConfiguration.class);

        when(config.getBoolean("spark.enable", false)).thenReturn(false);
        when(config.getBoolean("tez.enable", false)).thenReturn(false);

        FetcherFactory fetcherFactory = new FetcherFactory(config);
        fetcherFactory.getFetcher(FetcherType.TEZ);
    }
}
