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

import com.huawei.boostkit.omnituning.OmniTuningContext;
import io.ebean.Finder;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class BaseTestConfiguration {
    private static final String TESTING_CONFIG_FILE = "omniTuningConf.properties";
    private static final String ENCODING = StandardCharsets.UTF_8.displayName(Locale.ENGLISH);

    protected static PropertiesConfiguration testConfiguration;

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        Configurations configurations = new Configurations();
        URL configUrl = Thread.currentThread().getContextClassLoader().getResource(TESTING_CONFIG_FILE);
        FileBasedConfigurationBuilder.setDefaultEncoding(OmniTuningConfigure.class, ENCODING);
        testConfiguration = configurations.properties(configUrl);

        OmniTuningContext.initContext();
        OmniTuningContext context = OmniTuningContext.getInstance();
        Finder finder = Mockito.mock(Finder.class);
        when(finder.byId(any())).thenReturn(null);
        context.setFinder(finder);
    }
}
