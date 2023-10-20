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

import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;

import static org.mockito.Mockito.when;

public class TestTezClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String TEST_APP_STRING =
            "{" +
                "\"app\":[" +
                    "{" +
                        "\"appId\":\"application_test\"," +
                        "\"name\":\"sql_test\"," +
                        "\"appState\":\"FINISHED\"" +
                    "}" +
                "]" +
            "}";

    private static final String TEST_APP_INFO =
            "{" +
                "\"entitytype\":\"TEZ_APPLICATION\"," +
                "\"otherinfo\":{" +
                    "\"config\":{" +
                        "\"tez.am.resource.memory.mb\":1024," +
                        "\"tez.am.resource.cpu.vcores\":5," +
                        "\"tez.task.resource.memory.mb\":1024," +
                        "\"tez.task.reource.cpu.vcores\":5" +
                    "}" +
                "}" +
            "}";

    private static final String TEST_DAG_INFO =
            "{" +
                "\"entities\":[" +
                    "{" +
                        "\"entitytype\":\"TEZ_DAG_ID\"," +
                        "\"entity\":\"dag_test_1\"," +
                        "\"otherinfo\":{" +
                            "\"startTime\":0," +
                            "\"timeTaken\":100," +
                            "\"endTime\":100," +
                            "\"status\":\"SUCCEEDED\"" +
                        "}" +
                    "}" +
                "]" +
            "}";

    private static final String TEST_DAG_EXTRA_INFO =
            "{" +
                "\"entitytype\":\"TEZ_DAG_EXTRA_INFO\"," +
                "\"otherinfo\":{" +
                    "\"dagPlan\":{" +
                        "\"dagContext\":{" +
                            "\"description\":\"select * from table\"" +
                        "}" +
                    "}" +
                "}" +
            "}";

    public static TimelineClient getTestTimelineClient() throws IOException {
        TimelineClient testClient = Mockito.mock(TimelineClient.class);
        when(testClient.readJsonNode(new URL("http://testUrl:8188/ws/v1/applicationhistory/apps?applicationTypes=TEZ&startedTimeBegin=0&startedTimeEnd=100")))
                .thenReturn(MAPPER.readTree(TEST_APP_STRING));
        when(testClient.readJsonNode(new URL("http://testUrl:8188/ws/v1/timeline/TEZ_APPLICATION/tez_test")))
                .thenReturn(MAPPER.readTree(TEST_APP_INFO));
        when(testClient.readJsonNode(new URL("http://testUrl:8188/ws/v1/timeline/TEZ_DAG_ID?primaryFilter=applicationId:test")))
                .thenReturn(MAPPER.readTree(TEST_DAG_INFO));
        when((testClient.readJsonNode(new URL("http://testUrl:8188/ws/v1/timeline/TEZ_DAG_EXTRA_INFO/dag_test_1"))))
                .thenReturn(MAPPER.readTree(TEST_DAG_EXTRA_INFO));
        return testClient;
    }
}
