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

package com.huawei.boostkit.omniadvisor.dto;

import com.huawei.boostkit.omniadvisor.utils.MathUtils;

public class ApplicationResult {
    public static final long FAILED_JOB_DURATION = MathUtils.DAY_IN_MS;
    public static final String RESULT_TABLE_NAME = "history_config";
    public static final String SPARK_SUBMIT = "spark-submit";
    public static final String SPARK_SQL = "spark-sql";
    public static final int FAILED_STATUS = 0;
    public static final int SUCCEEDED_STATUS = 1;
    private String applicationId;

    private String applicationName;

    private String applicationWorkload;

    private long startTime;

    private long finishTime;

    private long durationTime;

    private String jobType;

    private String submit_method;

    private String deploy_mode;

    private String submit_cmd;

    private String parameters;

    private int executionStatus;

    private String query;

    private String identification;

    // scala不支持lombok,自己实现getter/setter
    public String getApplicationId() {
        return this.applicationId;
    }

    public String getApplicationName() {
        return this.applicationName;
    }

    public String getApplicationWorkload() {
        return this.applicationWorkload;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public long getDurationTime() {
        return this.durationTime;
    }

    public String getJobType() {
        return this.jobType;
    }

    public String getSubmit_method() {
        return this.submit_method;
    }

    public String getDeploy_mode() {
        return this.deploy_mode;
    }

    public String getSubmit_cmd() {
        return this.submit_cmd;
    }

    public String getParameters() {
        return this.parameters;
    }

    public int getExecutionStatus() {
        return this.executionStatus;
    }

    public String getQuery() {
        return this.query;
    }

    public String getIdentification() {
        return this.identification;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public void setApplicationWorkload(String applicationWorkload) {
        this.applicationWorkload = applicationWorkload;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public void setDurationTime(long durationTime) {
        this.durationTime = durationTime;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public void setSubmit_method(String submit_method) {
        this.submit_method = submit_method;
    }

    public void setDeploy_mode(String deploy_mode) {
        this.deploy_mode = deploy_mode;
    }

    public void setSubmit_cmd(String submit_cmd){
        this.submit_cmd = submit_cmd;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public void setExecutionStatus(int executionStatus) {
        this.executionStatus = executionStatus;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setIdentification(String identification) {
        this.identification = identification;
    }
}
