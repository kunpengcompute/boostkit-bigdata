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

package com.huawei.boostkit.omnituning.tez.utils;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omnituning.tez.data.TezDagIdData;
import com.huawei.boostkit.omnituning.utils.MathUtils;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.tez.dag.app.dag.DAGState;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class TestTezContext {
    public static final SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final String DATE_14 = "2023-09-02 14:00:00";
    public static final String DATE_15 = "2023-09-02 15:00:00";
    public static final String DATE_16 = "2023-09-02 16:00:00";
    public static final String DATE_17 = "2023-09-02 17:00:00";
    public static final String DATE_18 = "2023-09-02 18:00:00";

    public static final long TIME_14;
    public static final long TIME_15;
    public static final long TIME_16;
    public static final long TIME_17;
    public static final long TIME_18;

    static {
        try {
            TIME_14 = DF.parse(DATE_14).getTime();
            TIME_15 = DF.parse(DATE_15).getTime();
            TIME_16 = DF.parse(DATE_16).getTime();
            TIME_17 = DF.parse(DATE_17).getTime();
            TIME_18 = DF.parse(DATE_18).getTime();
        } catch (ParseException e) {
            throw new OmniTuningException("Parse time failed", e);
        }
    }

    public static final String SUCCESS = "success";
    public static final String FAILED = "failed";
    public static final String KILLED = "killed";
    public static final String UNFINISHED = "UNFINISHED";

    public static final AnalyticJob SUCCESS_JOB =
            new TezAnalyticJob(SUCCESS, SUCCESS, TIME_14, TIME_15, YarnApplicationState.FINISHED);
    public static final AnalyticJob FAILED_JOB =
            new TezAnalyticJob(FAILED, FAILED, TIME_15, TIME_16, YarnApplicationState.FINISHED);
    public static final AnalyticJob KILLED_JOB =
            new TezAnalyticJob(KILLED, KILLED, TIME_16, TIME_17, YarnApplicationState.KILLED);
    public static final AnalyticJob UNFINISHED_JOB =
            new TezAnalyticJob(UNFINISHED, UNFINISHED, TIME_17, TIME_18, YarnApplicationState.RUNNING);

    public static final TezDagIdData SUCCESS_DAG =
            new TezDagIdData(SUCCESS, TIME_14, TIME_15, MathUtils.HOUR_IN_MS, DAGState.SUCCEEDED);
    public static final TezDagIdData FAILED_DAG =
            new TezDagIdData(FAILED, TIME_15, TIME_16, MathUtils.HOUR_IN_MS, DAGState.FAILED);
    public static final TezDagIdData KILLED_DAG =
            new TezDagIdData(KILLED, TIME_16, TIME_17, MathUtils.HOUR_IN_MS, DAGState.RUNNING);
    public static final TezDagIdData UNFINISHED_DAG =
            new TezDagIdData(UNFINISHED, TIME_17, TIME_18, MathUtils.HOUR_IN_MS, DAGState.RUNNING);

    public static final List<AnalyticJob> TEST_APP_LIST =
            ImmutableList.of(SUCCESS_JOB, FAILED_JOB, KILLED_JOB, UNFINISHED_JOB);

    public static final String TEST_TEZ_QUERY = "select id, name from table";

    public static final Map<String, String> TEST_TEZ_CONFIGURE = ImmutableBiMap.of(
            "tez.am.resource.memory.mb", "200", "tez.am.resource.cpu.vcores", "2",
            "tez.task.resource.memory.mb", "300", "tez.task.resource.cpu.vcores", "4");
}
