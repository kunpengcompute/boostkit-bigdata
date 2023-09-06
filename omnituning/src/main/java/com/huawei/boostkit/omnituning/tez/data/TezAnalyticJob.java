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

package com.huawei.boostkit.omnituning.tez.data;

import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.fetcher.FetcherType;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

public class TezAnalyticJob implements AnalyticJob {
    private final String applicationId;
    private final String applicationName;
    private final long startTimeMills;
    private final long finishTimeMills;
    private final YarnApplicationState state;

    public TezAnalyticJob(String appId, String appName, long startTime, long finishTime, YarnApplicationState state) {
        this.applicationId = appId;
        this.applicationName = appName;
        this.startTimeMills = startTime;
        this.finishTimeMills = finishTime;
        this.state = state;
    }

    @Override
    public String getApplicationId() {
        return applicationId;
    }

    @Override
    public FetcherType getType() {
        return FetcherType.TEZ;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public long getStartTimeMills() {
        return startTimeMills;
    }

    public long getFinishTimeMills() {
        return finishTimeMills;
    }

    public YarnApplicationState getState() {
        return state;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof TezAnalyticJob)) {
            return false;
        }

        TezAnalyticJob otherJob = (TezAnalyticJob) other;
        return this.applicationId.equals(otherJob.applicationId)
                && this.applicationName.equals(otherJob.applicationName)
                && this.startTimeMills == otherJob.startTimeMills
                && this.finishTimeMills == otherJob.finishTimeMills;
    }
}
