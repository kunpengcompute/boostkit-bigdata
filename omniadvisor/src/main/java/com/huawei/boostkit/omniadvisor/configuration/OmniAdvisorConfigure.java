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

package com.huawei.boostkit.omniadvisor.configuration;

import org.apache.commons.configuration2.PropertiesConfiguration;

public class OmniAdvisorConfigure {
    private static final int DEFAULT_THREAD_COUNT = 3;
    private static final String THREAD_COUNT_CONF_KEY = "log.analyzer.thread.count";

    private final int threadCount;

    public OmniAdvisorConfigure(PropertiesConfiguration configuration) {
        this.threadCount = configuration.getInt(THREAD_COUNT_CONF_KEY, DEFAULT_THREAD_COUNT);
    }

    public int getThreadCount() {
        return threadCount;
    }
}
