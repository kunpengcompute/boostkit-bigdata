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

package com.huawei.boostkit.omniadvisor.models;

import com.huawei.boostkit.omniadvisor.utils.MathUtils;
import io.ebean.Model;
import io.ebean.annotation.Index;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = AppResult.RESULT_TABLE_NAME)
@Index(name = "history_config_i1", columnNames = {"application_id"})
@Index(name = "history_config_i2", columnNames = {"application_name"})
public class AppResult extends Model {
    private static final long serialVersionUID = 1L;

    public static final long FAILED_JOB_DURATION = MathUtils.DAY_IN_MS;
    public static final String RESULT_TABLE_NAME = "history_config";
    public static final String SPARK_SUBMIT = "spark-submit";
    public static final String SPARK_SQL = "spark-sql";
    public static final int FAILED_STATUS = 0;
    public static final int SUCCEEDED_STATUS = 1;
    private static final int APPLICATION_ID_LIMIT = 50;
    private static final int APPLICATION_NAME_LIMIT = 100;
    private static final int APPLICATION_WORKLOAD_LIMIT = 50;
    private static final int JOB_TYPE_LIMIT = 50;
    private static final int SUBMIT_METHOD_LIMIT = 50;
    private static final int DEPLOY_MODE_LIMIT = 50;
    private static final int IDENTIFICATION_LIMIT = 300;

    @Id
    @Column(length = APPLICATION_ID_LIMIT, unique = true, nullable = false)
    public String applicationId;

    @Column(length = APPLICATION_NAME_LIMIT, nullable = false)
    public String applicationName;

    @Column(length = APPLICATION_WORKLOAD_LIMIT)
    public String applicationWorkload;

    @Column()
    public long startTime;

    @Column()
    public long finishTime;

    @Column()
    public long durationTime;

    @Column(length = JOB_TYPE_LIMIT)
    public String jobType;

    @Column(length = SUBMIT_METHOD_LIMIT)
    public String submit_method;

    @Column(length = DEPLOY_MODE_LIMIT)
    public String deploy_mode;

    @Column(columnDefinition = "TEXT CHARACTER SET utf8mb4")
    public String submit_cmd;

    @Column(columnDefinition = "TEXT CHARACTER SET utf8mb4")
    public String parameters;

    @Column()
    public int executionStatus;

    @Column(columnDefinition = "TEXT CHARACTER SET utf8mb4")
    public String query;

    @Column(length = IDENTIFICATION_LIMIT)
    public String identification;
}
