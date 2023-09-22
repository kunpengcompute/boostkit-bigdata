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
import com.huawei.boostkit.omnituning.models.AppResult;
import io.ebean.Finder;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static java.lang.String.format;

public final class OmniTuningContext {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTuningContext.class);
    private static final String CONFIG_FILE_NAME = "omniTuningConf.properties";
    private static final String ENCODING = StandardCharsets.UTF_8.displayName();

    private static OmniTuningContext instance = null;

    private final OmniTuningConfigure omniTuningConfig;
    private final FetcherFactory fetcherFactory;

    private Finder<String, AppResult> finder = new Finder<>(AppResult.class);

    private OmniTuningContext() {
        this(false, null, null);
    }

    private OmniTuningContext(String user, String passwd) {
        this(true, user, passwd);
    }

    private OmniTuningContext(boolean initDatabase, String user, String passwd) {
        PropertiesConfiguration configuration = loadConfigure();
        if (initDatabase) {
            initDataSource(configuration, user, passwd);
        }
        this.omniTuningConfig = loadOmniTuningConfig(configuration);
        this.fetcherFactory = loadFetcherFactory(configuration);
    }

    public static void initContext(String user, String passwd) {
        if (instance == null) {
            instance = new OmniTuningContext(user, passwd);
        } else {
            LOG.warn("OmniTuningContext has been instantiated");
        }
    }

    // only use for unit test
    public static void initContext() {
        if (instance == null) {
            instance = new OmniTuningContext();
        } else {
            LOG.warn("OmniTuningContext has been instantiated");
        }
    }

    public static OmniTuningContext getInstance() {
        if (instance == null) {
            throw new OmniTuningException("OmniTuningContext has not been instantiated");
        }
        return instance;
    }

    public OmniTuningConfigure getOmniTuningConfig() {
        return omniTuningConfig;
    }

    public FetcherFactory getFetcherFactory() {
        return fetcherFactory;
    }

    public Finder<String, AppResult> getFinder() {
        return finder;
    }

    public void setFinder(Finder<String, AppResult> finder) {
        this.finder = finder;
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

    private void initDataSource(PropertiesConfiguration configuration, String user, String passwd) {
        DBConfigure.initDatabase(configuration, user, passwd);
    }

    private OmniTuningConfigure loadOmniTuningConfig(PropertiesConfiguration configuration) {
        return new OmniTuningConfigure(configuration);
    }

    private FetcherFactory loadFetcherFactory(PropertiesConfiguration configuration) {
        return new FetcherFactory(configuration);
    }
}
