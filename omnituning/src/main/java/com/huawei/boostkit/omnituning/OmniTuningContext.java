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

package com.huawei.boostkit.omnituning;

import com.huawei.boostkit.omnituning.configuration.DBConfigure;
import com.huawei.boostkit.omnituning.configuration.OmniTuningConfigure;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.fetcher.FetcherFactory;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static java.lang.String.format;

public final class OmniTuningContext {
    private static final String CONFIG_FILE_NAME = "omniTuningConf.properties";
    private static final String ENCODING = StandardCharsets.UTF_8.displayName();

    private static OmniTuningContext instance = null;

    private final OmniTuningConfigure omniTuningConfig;
    private final FetcherFactory fetcherFactory;

    private OmniTuningContext() {
        PropertiesConfiguration configuration = loadConfigure();
        initDataSource(configuration);
        this.omniTuningConfig = loadOmniTuningConfig(configuration);
        this.fetcherFactory = loadFetcherFactory(configuration);
    }

    public static OmniTuningContext getInstance() {
        if (instance == null) {
            instance = new OmniTuningContext();
        }
        return instance;
    }

    public OmniTuningConfigure getOmniTuningConfig() {
        return omniTuningConfig;
    }

    public FetcherFactory getFetcherFactory() {
        return fetcherFactory;
    }

    private PropertiesConfiguration loadConfigure() {
        try {
            Configurations configurations = new Configurations();
            URL configFileUrl = Thread.currentThread().getContextClassLoader().getResource(CONFIG_FILE_NAME);
            if (configFileUrl == null) {
                throw new OmniTuningException("Config file is missing");
            }
            FileBasedConfigurationBuilder.setDefaultEncoding(OmniTuningConfigure.class, ENCODING);
            return configurations.properties(configFileUrl);
        } catch (ConfigurationException e) {
            throw new OmniTuningException(format("Failed to read config file, %s", e));
        }
    }

    private void initDataSource(PropertiesConfiguration configuration) {
        DBConfigure.initDatabase(configuration);
    }

    private OmniTuningConfigure loadOmniTuningConfig(PropertiesConfiguration configuration) {
        return new OmniTuningConfigure(configuration);
    }

    private FetcherFactory loadFetcherFactory(PropertiesConfiguration configuration) {
        return new FetcherFactory(configuration);
    }
}
