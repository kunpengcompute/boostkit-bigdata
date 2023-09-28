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

package com.huawei.boostkit.omnituning.executor;

import com.huawei.boostkit.omnituning.OmniTuningContext;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.security.HadoopSecurity;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OmniTuningRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTuningRunner.class);

    private final long startTimeMills;
    private final long finishTimeMills;

    public OmniTuningRunner(long startTimeMills, long finishTimeMills) {
        this.startTimeMills = startTimeMills;
        this.finishTimeMills = finishTimeMills;
    }

    @Override
    public void run() {
        LOG.info("OmniTuning has started");
        try {
            Configuration hadoopConf = OmniTuningContext.getHadoopConfig();
            HadoopSecurity hadoopSecurity = new HadoopSecurity(hadoopConf);
            hadoopSecurity.doAs(new AnalysisAction(hadoopSecurity, startTimeMills, finishTimeMills));
        } catch (IOException e) {
            LOG.error("failed to analyze jobs", e);
            throw new OmniTuningException(e);
        }
    }
}
