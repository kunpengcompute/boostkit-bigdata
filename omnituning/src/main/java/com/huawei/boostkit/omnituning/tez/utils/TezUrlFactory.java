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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.history.logging.EntityTypes;

import javax.ws.rs.core.UriBuilder;

import java.net.MalformedURLException;
import java.net.URL;

import static java.lang.String.format;

public class TezUrlFactory {
    private static final String APPLICATION_TYPE = "TEZ";
    private static final String TEZ_APPLICATION_PREFIX = "tez_";

    private static final String APPLICATION_HISTORY_URL = "%s/ws/v1/applicationhistory/apps";
    private static final String TIMELINE_BASE_URL = "%s" + ATSConstants.RESOURCE_URI_BASE;
    private static final String TIMELINE_ENTITY_URL = TIMELINE_BASE_URL + "/%s/%s";
    private static final String TIMELINE_ENTITY_WITH_FILTER_URL = TIMELINE_BASE_URL + "/%s?primaryFilter=%s:%s";

    private final String baseUrl;

    public TezUrlFactory(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public URL getRootURL() throws MalformedURLException {
        return new URL(format(TIMELINE_BASE_URL, baseUrl));
    }

    public URL getApplicationURL(String applicationId) throws MalformedURLException {
        return new URL(format(TIMELINE_ENTITY_URL, baseUrl, EntityTypes.TEZ_APPLICATION,
                TEZ_APPLICATION_PREFIX + applicationId));
    }

    public URL getDagIdURL(String applicationId) throws MalformedURLException {
        return new URL(format(TIMELINE_ENTITY_WITH_FILTER_URL, baseUrl, EntityTypes.TEZ_DAG_ID,
                ATSConstants.APPLICATION_ID, applicationId));
    }

    public URL getDatExtraInfoURL(String dagId) throws MalformedURLException {
        return new URL(format(TIMELINE_ENTITY_URL, baseUrl, EntityTypes.TEZ_DAG_EXTRA_INFO, dagId));
    }

    public URL getApplicationHistoryURL(long startTime, long finishTime) throws MalformedURLException {
        return UriBuilder.fromUri(format(APPLICATION_HISTORY_URL, baseUrl))
                .queryParam(RMWSConsts.APPLICATION_TYPES, APPLICATION_TYPE)
                .queryParam(RMWSConsts.STARTED_TIME_BEGIN, startTime)
                .queryParam(RMWSConsts.STARTED_TIME_END, finishTime)
                .build().toURL();
    }
}
