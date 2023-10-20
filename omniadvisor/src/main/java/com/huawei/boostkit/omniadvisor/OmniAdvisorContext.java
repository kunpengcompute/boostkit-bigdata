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

package com.huawei.boostkit.omniadvisor;

import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.omniadvisor.configuration.DBConfigure;
import com.huawei.boostkit.omniadvisor.configuration.OmniAdvisorConfigure;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherFactory;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import io.ebean.Finder;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;

public final class OmniAdvisorContext {
    private static final Logger LOG = LoggerFactory.getLogger(OmniAdvisorContext.class);
    private static final String CONFIG_FILE_NAME = "omniAdvisorLogAnalyzer.properties";
    private static final List<String> DEFAULT_HADOOP_CONFIG_FILES = ImmutableList.of("hdfs-site.xml", "core-site.xml");
    private static final String ENCODING = StandardCharsets.UTF_8.displayName(Locale.ENGLISH);
    private static final Configuration HADOOP_CONF;

    private static OmniAdvisorContext instance = null;

    private final OmniAdvisorConfigure omniAdvisorConfigure;
    private final FetcherFactory fetcherFactory;

    static {
        HADOOP_CONF = new Configuration();
        for (String configFileName : DEFAULT_HADOOP_CONFIG_FILES) {
            URL configFile = Thread.currentThread().getContextClassLoader().getResource(configFileName);
            if (configFile != null) {
                LOG.info("Add resource {} to hadoop config", configFile);
                HADOOP_CONF.addResource(configFile);
            }
        }
    }

    private Finder<String, AppResult> finder = new Finder<>(AppResult.class);

    private OmniAdvisorContext() {
        this(false, null, null);
    }

    private OmniAdvisorContext(String user, String passwd) {
        this(true, user, passwd);
    }

    private OmniAdvisorContext(boolean initDatabase, String user, String passwd) {
        PropertiesConfiguration configuration = loadConfigure();
        if (initDatabase) {
            initDataSource(configuration, user, passwd);
        }
        this.omniAdvisorConfigure = loadOmniTuningConfig(configuration);
        this.fetcherFactory = loadFetcherFactory(configuration);
    }

    public static void initContext(String user, String passwd) {
        if (instance == null) {
            instance = new OmniAdvisorContext(user, passwd);
        } else {
            LOG.warn("OmniTuningContext has been instantiated");
        }
    }

    // only use for unit test
    public static void initContext() {
        if (instance == null) {
            instance = new OmniAdvisorContext();
        } else {
            LOG.warn("OmniTuningContext has been instantiated");
        }
    }

    public static OmniAdvisorContext getInstance() {
        if (instance == null) {
            throw new OmniAdvisorException("OmniTuningContext has not been instantiated");
        }
        return instance;
    }

    public static Configuration getHadoopConfig() {
        return HADOOP_CONF;
    }

    public OmniAdvisorConfigure getOmniAdvisorConfigure() {
        return omniAdvisorConfigure;
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
                throw new OmniAdvisorException("Config file is missing");
            }
            FileBasedConfigurationBuilder.setDefaultEncoding(OmniAdvisorConfigure.class, ENCODING);
            return configurations.properties(configFileUrl);
        } catch (ConfigurationException e) {
            throw new OmniAdvisorException(format("Failed to read config file, %s", e));
        }
    }

    private void initDataSource(PropertiesConfiguration configuration, String user, String passwd) {
        DBConfigure.initDatabase(configuration, user, passwd);
    }

    private OmniAdvisorConfigure loadOmniTuningConfig(PropertiesConfiguration configuration) {
        return new OmniAdvisorConfigure(configuration);
    }

    private FetcherFactory loadFetcherFactory(PropertiesConfiguration configuration) {
        return new FetcherFactory(configuration);
    }
}
