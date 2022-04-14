/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.plugin.hive.rule;

import io.prestosql.plugin.hive.omnidata.OmniDataNodeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationOmniDataConfig;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.unsimulationOmniDataConfig;

public class TestHivePushdown
{
    @BeforeClass
    public void load()
    {
        simulationOmniDataConfig();
        OmniDataNodeManager manager = new OmniDataNodeManager();
        HivePushdownUtil.setOmniDataNodeManager(manager);
    }

    @AfterClass
    public void unload()
    {
        unsimulationOmniDataConfig();
    }
}
