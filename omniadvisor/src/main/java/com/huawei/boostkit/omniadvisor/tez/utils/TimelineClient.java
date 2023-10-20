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

import com.google.common.annotations.VisibleForTesting;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.TimelineReaderFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URL;

import static java.lang.String.format;

public class TimelineClient implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Client httpClient;

    public TimelineClient(Configuration conf, boolean useHttps, int connTimeout) {
        try {
            this.httpClient = TimelineReaderFactory.getTimelineReaderStrategy(conf, useHttps, connTimeout).getHttpClient();
        } catch (TezException | IOException e) {
            throw new OmniAdvisorException(e);
        }
    }

    public JsonNode readJsonNode(URL url) {
        WebResource resource = httpClient.resource(url.toString());
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON_TYPE)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            try {
                return MAPPER.readTree(response.getEntity(JSONObject.class).toString());
            } catch (IOException e) {
                throw new OmniAdvisorException(e);
            }
        } else {
            throw new OmniAdvisorException(format("Failed to get data from %s", url));
        }
    }

    @VisibleForTesting
    protected void setClient(Client client) {
        this.httpClient = client;
    }

    @Override
    public void close() {
        httpClient.destroy();
    }
}
