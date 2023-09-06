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

package com.huawei.boostkit.omnituning.configuration;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.BeforeClass;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class TestConfiguration {
    private static final String TESTING_CONFIG_FILE_NAME = "TestingConfigure.properties";
    private static final String TESTING_SPARK_CONFIG_FILE_NAME = "TestingSparkConfigure.properties";
    private static final String ENCODING = StandardCharsets.UTF_8.displayName();

    protected static PropertiesConfiguration testConfiguration;
    protected static PropertiesConfiguration testSparkConfiguration;

    @BeforeClass
    public static void setUpClass() throws ConfigurationException {
        Configurations configurations = new Configurations();
        URL configFileUrl = Thread.currentThread().getContextClassLoader().getResource(TESTING_CONFIG_FILE_NAME);
        URL sparkConfig = Thread.currentThread().getContextClassLoader().getResource(TESTING_SPARK_CONFIG_FILE_NAME);
        FileBasedConfigurationBuilder.setDefaultEncoding(OmniTuningConfigure.class, ENCODING);
        testConfiguration = configurations.properties(configFileUrl);
        testSparkConfiguration = configurations.properties(sparkConfig);
    }
}
