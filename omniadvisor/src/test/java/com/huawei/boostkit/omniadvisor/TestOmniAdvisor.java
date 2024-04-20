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

import com.huawei.boostkit.omniadvisor.configuration.BaseTestConfiguration;
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestOmniAdvisor extends BaseTestConfiguration {
    @Test
    public void testOmniTuning() {
        OmniAdvisor.main(new String[]{"2020-09-02 00:00:00", "2020-09-02 00:00:00", "user", "passwd"});
    }

    @Test(expected = OmniAdvisorException.class)
    public void testErrorNumberParams() {
        OmniAdvisor.main(new String[]{"2020-09-02 00:00:00", "2020-09-02 00:00:00"});
    }

    @Test(expected = OmniAdvisorException.class)
    public void testErrorTimeParser() {
        OmniAdvisor.main(new String[]{"2020-09-02 00-00-00", "2020-09-02 00-00-00", "user", "pass"});
    }

    @Test(expected = OmniAdvisorException.class)
    public void testErrorTimeOrder() {
        OmniAdvisor.main(new String[]{"2020-09-02 20:00:00", "2020:09:02 00-00-00", "user", "pass"});
    }

    @Test
    public void testAnalysisLog() {
        OmniAdvisor advisor = new OmniAdvisor();
        String startTime = "2024-04-17 10:35:40";
        String finishedTime = "2024-04-17 10:45:00";
        Map<String, ApplicationResult> applicationResultMap = advisor.analysisLog(null, startTime, finishedTime);
        Assert.assertNotNull(applicationResultMap);
    }
}
