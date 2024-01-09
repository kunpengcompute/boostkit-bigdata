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

package com.huawei.boostkit.omniadvisor.tez.utils;

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omniadvisor.tez.data.TezDagIdData;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

public class TezJsonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TezJsonUtils.class);
    private static final String HIVE_APP_NAME_PREFIX = "HIVE-";

    private final TezUrlFactory tezUrlFactory;
    private TimelineClient timelineClient;

    public TezJsonUtils(TezUrlFactory tezUrlFactory, boolean useHttps, int timeout) {
        this.tezUrlFactory = tezUrlFactory;
        this.timelineClient = new TimelineClient(OmniAdvisorContext.getHadoopConfig(), useHttps, timeout);
    }

    public void verifyTimeLineServer() throws IOException {
        URL timeLineUrl = tezUrlFactory.getRootURL();
        URLConnection connection = timeLineUrl.openConnection();
        connection.connect();
    }

    public List<AnalyticJob> getApplicationJobs(long startedTime, long finishedTime)
            throws IOException, AuthenticationException {
        URL historyUrl = tezUrlFactory.getApplicationHistoryURL(startedTime, finishedTime);
        LOG.info("calling REST API at {} to get applications", historyUrl.toString());
        JsonNode rootNode = timelineClient.readJsonNode(historyUrl);
        JsonNode apps = rootNode.path("app");
        List<AnalyticJob> analyticJobs = new ArrayList<>();
        for (JsonNode app : apps) {
            String appId = app.get(RMWSConsts.APP_ID).getTextValue();
            if (OmniAdvisorContext.getInstance().getFinder().byId(appId) == null) {
                String name = getApplicationName(app.get("name").getTextValue());
                String state = app.get("appState").getTextValue();
                TezAnalyticJob tezJob =
                        new TezAnalyticJob(appId, name, startedTime, finishedTime, YarnApplicationState.valueOf(state));
                analyticJobs.add(tezJob);
            }
        }
        return analyticJobs;
    }

    private String getApplicationName(String name) {
        if (name.startsWith(HIVE_APP_NAME_PREFIX)) {
            return name.substring(HIVE_APP_NAME_PREFIX.length());
        } else {
            return name;
        }
    }

    public List<TezDagIdData> getDAGIds(String applicationId) throws MalformedURLException {
        URL dagIdUrl = tezUrlFactory.getDagIdURL(applicationId);
        LOG.info("Get DAG ids from REST API at {}", dagIdUrl.toString());
        JsonNode rootNode = timelineClient.readJsonNode(dagIdUrl);
        List<TezDagIdData> dagIds = new ArrayList<>();

        for (JsonNode entity : rootNode.get(ATSConstants.ENTITIES)) {
            String dagId = entity.get(ATSConstants.ENTITY).getTextValue();
            long startTime = entity.get(ATSConstants.OTHER_INFO).path(ATSConstants.START_TIME).getLongValue();
            long endTime = entity.get(ATSConstants.OTHER_INFO).path(ATSConstants.FINISH_TIME).getLongValue();
            long duration = entity.get(ATSConstants.OTHER_INFO).path(ATSConstants.TIME_TAKEN).getLongValue();
            DAGState status =
                    DAGState.valueOf(entity.path(ATSConstants.OTHER_INFO).path(ATSConstants.STATUS).getTextValue());
            dagIds.add(new TezDagIdData(dagId, startTime, endTime, duration, status));
        }
        LOG.info("Get {} dags for application {}", dagIds.size(), applicationId);
        return dagIds;
    }

    public Map<String, String> getConfigure(String applicationId) throws MalformedURLException {
        URL applicationURL = tezUrlFactory.getApplicationURL(applicationId);
        LOG.info("Get configuration by calling REST API {}", applicationURL);
        JsonNode rootNode = timelineClient.readJsonNode(applicationURL);
        JsonNode config = rootNode.path(ATSConstants.OTHER_INFO).path(ATSConstants.CONFIG);
        Iterator<String> fieldNames = config.getFieldNames();
        Map<String, String> params = new HashMap<>();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            String value = config.get(key).getTextValue();
            params.put(key, value);
        }
        return params;
    }

    public String getQueryString(List<TezDagIdData> dagIds) throws MalformedURLException {
        StringJoiner joiner = new StringJoiner(";\n");
        for (TezDagIdData dagIdData : dagIds) {
            String singleQueryString = getSingleQueryString(dagIdData.getDagId());
            joiner.add(singleQueryString);
        }
        return joiner.toString();
    }

    public String getSingleQueryString(String dagId) throws MalformedURLException {
        URL dagExtraInfoURL = tezUrlFactory.getDagExtraInfoURL(dagId);
        LOG.info("Get query string for Dag {} by calling REST API {}", dagId, dagExtraInfoURL);
        JsonNode rootNode = timelineClient.readJsonNode(dagExtraInfoURL);
        return rootNode.path(ATSConstants.OTHER_INFO)
                .path(ATSConstants.DAG_PLAN)
                .path(DAGUtils.DAG_CONTEXT_KEY)
                .get(ATSConstants.DESCRIPTION)
                .getTextValue();
    }

    public void setTimelineClient(TimelineClient timelineClient) {
        this.timelineClient = timelineClient;
    }
}
