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

import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;

public class TestJsonUtils {
    private static TezJsonUtils testJsonUtils;

    @BeforeClass
    public static void setUpClass() {
        testJsonUtils = new TezJsonUtils(new TezUrlFactory("http://localhost:9999"), false, 10);
    }

    @Test(expected = ConnectException.class)
    public void testVerifyTimeLineServer() throws IOException {
        testJsonUtils.verifyTimeLineServer();
    }

    @Test(expected = ClientHandlerException.class)
    public void testGetApplicationJobs() throws AuthenticationException, IOException {
        testJsonUtils.getApplicationJobs(0L, 1000L, null);
    }

    @Test(expected = ClientHandlerException.class)
    public void testGetDAGIDs() throws MalformedURLException {
        testJsonUtils.getDAGIds("appId");
    }

    @Test(expected = ClientHandlerException.class)
    public void testGetConfigure() throws MalformedURLException {
        testJsonUtils.getConfigure("appId");
    }

    @Test(expected = ClientHandlerException.class)
    public void testGetQueryString() throws MalformedURLException {
        testJsonUtils.getSingleQueryString("appId");
    }
}
