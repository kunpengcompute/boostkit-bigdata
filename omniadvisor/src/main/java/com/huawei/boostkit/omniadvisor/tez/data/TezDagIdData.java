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

package com.huawei.boostkit.omniadvisor.tez.data;

import org.apache.tez.dag.app.dag.DAGState;

public class TezDagIdData implements Comparable<TezDagIdData> {
    private final String dagId;
    private final long startTime;
    private final long endTime;
    private final long duration;
    private final DAGState status;

    public TezDagIdData(String dagId, long startTime, long endTime, long duration, DAGState status) {
        this.dagId = dagId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.status = status;
    }

    public String getDagId() {
        return dagId;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getDuration() {
        return duration;
    }

    public boolean isComplete() {
        return (status == DAGState.SUCCEEDED ||
                status == DAGState.FAILED ||
                status == DAGState.KILLED ||
                status == DAGState.ERROR ||
                status == DAGState.TERMINATING);
    }

    public boolean isSuccess() {
        return status == DAGState.SUCCEEDED;
    }

    @Override
    public int compareTo(TezDagIdData other) {
        return Long.compare(this.startTime, other.startTime);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TezDagIdData)) {
            return false;
        }
        return this.dagId.equals(((TezDagIdData) other).dagId);
    }
}
