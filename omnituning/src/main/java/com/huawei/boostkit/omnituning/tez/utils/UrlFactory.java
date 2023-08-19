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

package com.huawei.boostkit.omnituning.tez.utils;

import javax.ws.rs.core.UriBuilder;

import java.net.MalformedURLException;
import java.net.URL;

import static java.lang.String.format;

public class UrlFactory {
    private static final String APPLICATION_TYPE = "TEZ";
    private static final String APPLICATION_HISTORY_URL = "%s/ws/v1/applicationhistory/apps";
    private static final String TIMELINE_BASE_URL = "%s/ws/v1/timeline";
    private static final String TEZ_APPLICATION_URL = TIMELINE_BASE_URL + "/TEZ_APPLICATION/tez_%s";
    private static final String TEZ_DAG_URL =TIMELINE_BASE_URL + "/TEZ_DAG_ID/%s";
    private static final String TEZ_DAG_ID_URL = TIMELINE_BASE_URL + "/TEZ_DAG_ID/%s";
    private static final String TEZ_DAG_EXTRA_INFO_URL = TIMELINE_BASE_URL + "/TEZ_DAG_EXTRA_INFO/%s";
    private static final String TEZ_VERTEX_URL = TIMELINE_BASE_URL + "/TEZ_VERTEX_ID?primaryFilter=TEZ_DAG_ID:%s";
    private static final String TASK_URL = TIMELINE_BASE_URL + "/TEZ_TASK_ID/%s";
    private static final String TASK_LIST_BY_VERTEX_URL = TIMELINE_BASE_URL
            + "TEZ_TASK_ID?primaryFilter=TEZ_DAG_ID:%s&secondaryFilter=TEZ_VERTEX_ID:%s";
    private static final String TASK_ATTEMPT_URL = TIMELINE_BASE_URL
            + "/TEZ_TASK_ATTEMPT_ID?primaryFilter=TEZ_DAG_ID:%s&secondaryFilter=TEZ_TASK_ID:%s";
    private static final String TASK_ATTEMPT_ID_URL = TIMELINE_BASE_URL + "/TEZ_TASK_ATTEMPT_ID/%s";

    private final String baseUrl;

    public UrlFactory(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public URL getRootURL() throws MalformedURLException {
        return new URL(format(TIMELINE_BASE_URL, baseUrl));
    }

    public URL getApplicationURL(String applicationId) throws MalformedURLException {
        return new URL(format(TEZ_APPLICATION_URL, baseUrl, applicationId));
    }

    public URL getDagIdURL(String applicationId) throws MalformedURLException {
        return new URL(format(TEZ_DAG_ID_URL, baseUrl, applicationId));
    }

    public URL getDagURL(String dagId) throws MalformedURLException {
        return new URL(format(TEZ_DAG_URL, baseUrl, dagId));
    }

    public URL getDatExtraInfoURL(String dagId) throws MalformedURLException {
        return new URL(format(TEZ_DAG_EXTRA_INFO_URL, baseUrl, dagId));
    }

    public URL getVertexListURL(String dagId) throws MalformedURLException {
        return new URL(format(TEZ_VERTEX_URL, baseUrl, dagId));
    }

    public URL getTaskListByVertexURL(String dagId, String vertexId) throws MalformedURLException {
        return new URL(format(TASK_LIST_BY_VERTEX_URL, baseUrl, dagId, vertexId));
    }

    public URL getTaskURL(String taskId) throws MalformedURLException {
        return new URL(format(TASK_URL, baseUrl, taskId));
    }

    public URL getTaskAllAttemptsURL(String dagId, String taskId) throws MalformedURLException {
        return new URL(format(TASK_ATTEMPT_URL, baseUrl, dagId, taskId));
    }

    public URL getTaskAttemptURL(String attemptId) throws MalformedURLException {
        return new URL(format(TASK_ATTEMPT_ID_URL, baseUrl, attemptId));
    }

    public URL getApplicationHistoryURL(long startTime, long finishTime) throws MalformedURLException {
        return UriBuilder.fromUri(format(APPLICATION_HISTORY_URL, baseUrl))
                .queryParam("applicationTypes", APPLICATION_TYPE)
                .queryParam("startedTimeBegin", startTime)
                .queryParam("finishedTimeEnd", finishTime)
                .build().toURL();
    }
}
