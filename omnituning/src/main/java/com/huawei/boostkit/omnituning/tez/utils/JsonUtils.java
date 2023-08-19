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

import com.huawei.boostkit.omnituning.analysis.AnalyticJob;
import com.huawei.boostkit.omnituning.models.AppResult;
import com.huawei.boostkit.omnituning.tez.data.TezAnalyticJob;
import com.huawei.boostkit.omnituning.tez.data.TezCounterData;
import com.huawei.boostkit.omnituning.tez.data.TezTaskData;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.huawei.boostkit.omnituning.tez.utils.TimelineUtils.readJsonNode;

public class JsonUtils {
    private final UrlFactory urlFactory;

    public JsonUtils(UrlFactory urlFactory) {
        this.urlFactory = urlFactory;
    }

    public void verifyTimeLineServer() throws IOException {
        URL timeLineUrl = urlFactory.getRootURL();
        URLConnection connection = timeLineUrl.openConnection();
        connection.connect();
    }

    public List<AnalyticJob> getApplicationJobs(long startedTime, long finishedTime)
            throws IOException, AuthenticationException {
        URL historyUrl = urlFactory.getApplicationHistoryURL(startedTime, finishedTime);

        JsonNode rootNode = readJsonNode(historyUrl);
        JsonNode apps = rootNode.path("app");
        List<AnalyticJob> analyticJobs = new ArrayList<>();
        for (JsonNode app : apps) {
            String appId = app.get("appId").getTextValue();
            if (AppResult.FINDER.byId(appId) == null) {
                String trackingUrl = app.get("trackingUrl").getTextValue();
                long startTime = app.get("startedTime").getLongValue();
                long finishTime = app.get("finishedTime").getLongValue();
                boolean isSuccess = app.get("finalAppStatus").getTextValue().equals("SUCCEEDED");
                TezAnalyticJob tezJob = new TezAnalyticJob(appId, trackingUrl, startTime, finishTime, isSuccess);
                analyticJobs.add(tezJob);
            }
        }
        return analyticJobs;
    }

    public List<String> getDAGIds(String applicationId) throws IOException, AuthenticationException {
        URL dagIdUrl = urlFactory.getDagIdURL(applicationId);
        JsonNode rootNode = readJsonNode(dagIdUrl);
        List<String> dagIds = new ArrayList<>();

        for (JsonNode entity : rootNode.get("entities")) {
            String dagId = entity.get("entity").getTextValue();
            dagIds.add(dagId);
        }

        return dagIds;
    }

    public boolean isDagSuccess(String dagId) throws IOException, AuthenticationException {
        URL dagURL = urlFactory.getDagURL(dagId);
        JsonNode rootNode = readJsonNode(dagURL);
        return rootNode.path("otherinfo").path("status").getTextValue().equals("SUCCEEDED");
    }

    public Map<String, String> getConfigure(String applicationId) throws IOException, AuthenticationException {
        URL applicationURL = urlFactory.getApplicationURL(applicationId);
        JsonNode rootNode = readJsonNode(applicationURL);
        Map<String, String> params = new HashMap<>();
        JsonNode config = rootNode.path("otherinfo").path("config");
        Iterator<String> fieldNames = config.getFieldNames();
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            String value = config.get(key).getTextValue();
            params.put(key, value);
        }
        return params;
    }

    public String getQueryString(String dagId) throws IOException, AuthenticationException {
        URL dagExtraInfoURL = urlFactory.getDatExtraInfoURL(dagId);
        JsonNode rootNode = readJsonNode(dagExtraInfoURL);
        return rootNode.path("otherinfo").path("dagPlan").path("dagContext").get("description").getTextValue();
    }

    public void getAllTaskData(String dagId, List<TezTaskData> mapperTasks, List<TezTaskData> reducerTasks)
            throws IOException, AuthenticationException {
        URL vertexListUrl = urlFactory.getVertexListURL(dagId);
        JsonNode rootVertexNode = readJsonNode(vertexListUrl);
        JsonNode vertices = rootVertexNode.path("entities");
        for (JsonNode vertex : vertices) {
            String vertexId = vertex.get("entity").getTextValue();
            String vertexClass = vertex.get("otherinfo").path("processorClassName").getTextValue();
            URL tasksByVertexURL = urlFactory.getTaskListByVertexURL(dagId, vertexId);
            if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.MapTezProcessor")) {
                getTaskDataByVertexId(tasksByVertexURL, dagId, mapperTasks, true);
            }
            if (vertexClass.equals("org.apache.hadoop.hive.ql.exec.tez.ReduceTezProcessor")) {
                getTaskDataByVertexId(tasksByVertexURL, dagId, reducerTasks, false);
            }
        }
    }

    public void getTaskDataByVertexId(URL url, String dagId, List<TezTaskData> taskList, boolean isMapTask)
            throws IOException, AuthenticationException {

        JsonNode rootNode = readJsonNode(url);
        JsonNode tasks = rootNode.path("entities");
        for (JsonNode task : tasks) {
            String state = task.path("otherinfo").path("status").getTextValue();
            String taskId = task.get("entity").getValueAsText();
            String attemptId = task.path("otherinfo").path("successfulAttemptId").getTextValue();
            if (state.equals("SUCCEEDED")) {
                attemptId = task.path("otherinfo").path("successfulAttemptId").getTextValue();
            } else {
                JsonNode firstAttempt = getTaskFirstFailedAttempt(urlFactory.getTaskAllAttemptsURL(dagId,taskId));
                if(firstAttempt != null){
                    attemptId = firstAttempt.get("entity").getTextValue();
                }
            }

            taskList.add(new TezTaskData(taskId, attemptId));
        }

        getTaskData(dagId, taskList, isMapTask);

    }

    private JsonNode getTaskFirstFailedAttempt(URL taskAllAttemptsUrl) throws IOException, AuthenticationException {
        JsonNode rootNode = readJsonNode(taskAllAttemptsUrl);
        long firstAttemptFinishTime = Long.MAX_VALUE;
        JsonNode firstAttempt = null;
        JsonNode taskAttempts = rootNode.path("entities");
        for (JsonNode taskAttempt : taskAttempts) {
            String state = taskAttempt.path("otherinfo").path("counters").path("status").getTextValue();
            if (state.equals("SUCCEEDED")) {
                continue;
            }
            long finishTime = taskAttempt.path("otherinfo").path("counters").path("endTime").getLongValue();
            if( finishTime < firstAttemptFinishTime) {
                firstAttempt = taskAttempt;
                firstAttemptFinishTime = finishTime;
            }
        }
        return firstAttempt;
    }



    private void getTaskData(String dagId, List<TezTaskData> taskList, boolean isMapTask)
            throws IOException, AuthenticationException {
        for (TezTaskData data : taskList) {
            URL taskCounterURL = urlFactory.getTaskURL(data.getTaskId());
            TezCounterData taskCounter = getTaskCounter(taskCounterURL);

            URL taskAttemptURL = urlFactory.getTaskAttemptURL(dagId);
            long[] taskExecTime = getTaskExecTime(taskAttemptURL, isMapTask);

            data.setCounter(taskCounter);
            data.setTime(taskExecTime);
        }

    }

    private TezCounterData getTaskCounter(URL url) throws IOException, AuthenticationException {
        JsonNode rootNode = readJsonNode(url);
        JsonNode groups = rootNode.path("otherinfo").path("counters").path("counterGroups");
        TezCounterData holder = new TezCounterData();

        //Fetch task level metrics
        for (JsonNode group : groups) {
            for (JsonNode counter : group.path("counters")) {
                String name = counter.get("counterName").getTextValue();
                String groupName = group.get("counterGroupName").getTextValue();
                long value = counter.get("counterValue").getLongValue();
                holder.set(groupName, name, value);
            }
        }

        return holder;
    }

    private long[] getTaskExecTime(URL url, boolean isMapTask) throws IOException, AuthenticationException {
        JsonNode rootNode = readJsonNode(url);
        JsonNode groups = rootNode.path("otherinfo").path("counters").path("counterGroups");

        long startTime = rootNode.path("otherinfo").get("startTime").getLongValue();
        long finishTime = rootNode.path("otherinfo").get("endTime").getLongValue();

        long shuffleTime = 0;
        long mergeTime = 0;

        for (JsonNode group : groups) {
            for (JsonNode counter : group.path("counters")) {
                String name = counter.get("counterName").getTextValue();
                if (!isMapTask && name.equals("MERGE_PHASE_TIME")) {
                    mergeTime = counter.get("counterValue").getLongValue();
                } else if (!isMapTask && name.equals("SHUFFLE_PHASE_TIME")){
                    shuffleTime = counter.get("counterValue").getLongValue();
                }
            }
        }

        return new long[] { finishTime - startTime, shuffleTime, mergeTime, startTime, finishTime };
    }
}
