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

package com.huawei.boostkit.omniadvisor.executor;

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.huawei.boostkit.omniadvisor.security.HadoopSecurity;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OmniAdvisorRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(OmniAdvisorRunner.class);

    private final long startTimeMills;
    private final long finishTimeMills;
    private final List<String> analyzedList;

    public OmniAdvisorRunner(long startTimeMills, long finishTimeMills, List<String> analyzedList) {
        this.startTimeMills = startTimeMills;
        this.finishTimeMills = finishTimeMills;
        this.analyzedList = analyzedList;
    }

    @Override
    public void run() {
        LOG.info("OmniAdvisor has started");
        try {
            Configuration hadoopConf = OmniAdvisorContext.getHadoopConfig();
            HadoopSecurity hadoopSecurity = new HadoopSecurity(hadoopConf);
            hadoopSecurity.doAs(new AnalysisAction(hadoopSecurity, startTimeMills, finishTimeMills, analyzedList));
        } catch (IOException e) {
            LOG.error("failed to analyze jobs", e);
            throw new OmniAdvisorException(e);
        }
    }
}
