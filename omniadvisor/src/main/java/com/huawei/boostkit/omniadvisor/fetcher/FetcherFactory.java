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

package com.huawei.boostkit.omniadvisor.fetcher;

import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.spark.SparkFetcher;
import com.huawei.boostkit.omniadvisor.tez.TezFetcher;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class FetcherFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FetcherFactory.class);

    private final Map<FetcherType, Fetcher> enabledFetchers;

    public FetcherFactory(PropertiesConfiguration configuration) {
        Map<FetcherType, Fetcher> fetchers = new HashMap<>();

        // init TEZ fetcher
        Fetcher tezFetcher = new TezFetcher(configuration);
        if (tezFetcher.isEnable()) {
            LOG.info("TEZ Fetcher is enabled.");
            fetchers.put(FetcherType.TEZ, tezFetcher);
        }

        // init SPARK fetcher
        Fetcher sparkFetcher = new SparkFetcher(configuration);
        if (sparkFetcher.isEnable()) {
            LOG.info("Spark Fetcher is enabled.");
            fetchers.put(FetcherType.SPARK, sparkFetcher);
        }

        this.enabledFetchers = fetchers;
    }

    public Fetcher getFetcher(FetcherType type) {
        if (enabledFetchers.containsKey(type)) {
            return enabledFetchers.get(type);
        } else {
            throw new OmniAdvisorException(format("Fetcher [%s] is disabled", type.getName()));
        }
    }

    public List<Fetcher> getAllFetchers() {
        return ImmutableList.copyOf(enabledFetchers.values());
    }

    public void addFetcher(FetcherType type, Fetcher fetcher) {
        enabledFetchers.put(type, fetcher);
    }
}
