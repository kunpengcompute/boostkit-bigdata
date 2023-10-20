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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestTimelineClient {
    @Test
    public void testReadJsonNode() throws MalformedURLException, JSONException {
        try (TimelineClient timelineClient = new TimelineClient(new Configuration(), false, 6000)) {
            String testUrl = "http://test-url:8188/test";

            ClientResponse response = Mockito.mock(ClientResponse.class);
            when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
            JSONObject jsonObject = new JSONObject("{\"name\" : \"test\"}");
            when(response.getEntity(JSONObject.class)).thenReturn(jsonObject);
            WebResource resource = Mockito.mock(WebResource.class);
            WebResource.Builder builder = Mockito.mock(WebResource.Builder.class);
            when(resource.accept(MediaType.APPLICATION_JSON_TYPE)).thenReturn(builder);
            when(builder.type(MediaType.APPLICATION_JSON_TYPE)).thenReturn(builder);
            when(builder.get(ClientResponse.class)).thenReturn(response);

            Client httpClient = Mockito.mock(Client.class);
            when(httpClient.resource(testUrl)).thenReturn(resource);
            timelineClient.setClient(httpClient);
            JsonNode object = timelineClient.readJsonNode(new URL(testUrl));
            assertEquals(object.get("name").getTextValue(), "test");
        }
    }
}
