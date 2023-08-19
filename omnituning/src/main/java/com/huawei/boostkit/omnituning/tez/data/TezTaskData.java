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

public class TezTaskData {
    private TezCounterData counterHolder;
    private String taskId;
    private String attemptId;

    private long totalTimeMs = 0;
    private long shuffleTimeMs = 0;
    private long sortTimeMs = 0;
    private long startTime = 0;
    private long finishTime = 0;
    private boolean isSampled = false;

    public TezTaskData(TezCounterData counterHolder, long[] time) {
        if(time == null || time.length < 3){
            time = new long[5];
        }
        this.counterHolder = counterHolder;
        this.totalTimeMs = time[0];
        this.shuffleTimeMs = time[1];
        this.sortTimeMs = time[2];
        if (time.length > 3) {
            this.startTime = time[3];
        }
        if (time.length > 4) {
            this.finishTime = time[4];
        }
        this.isSampled = true;
    }

    public TezTaskData(TezCounterData counterHolder) {
        this.counterHolder = counterHolder;
    }

    public TezTaskData(String taskId, String taskAttemptId) {
        this.taskId = taskId;
        this.attemptId = taskAttemptId;
    }

    public void setCounter(TezCounterData counterHolder) {
        this.counterHolder = counterHolder;
        this.isSampled = true;
    }

    public void setTime(long[] time) {
        this.totalTimeMs = time[0];
        this.shuffleTimeMs = time[1];
        this.sortTimeMs = time[2];
        this.startTime = time[3];
        this.finishTime = time[4];
        this.isSampled = true;
    }

    public void setTimeAndCounter(long[] time, TezCounterData counterHolder){
        if(time == null || time.length<3){
            time = new long[5];
        }
        this.totalTimeMs = time[0];
        this.shuffleTimeMs = time[1];
        this.sortTimeMs = time[2];
        if (time.length > 3) {
            this.startTime = time[3];
        }
        if (time.length > 4) {
            this.finishTime = time[4];
        }
        this.isSampled = true;
        this.counterHolder = counterHolder;
    }


    public String getTaskId() {
        return taskId;
    }

}