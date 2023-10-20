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

package com.huawei.boostkit.omniadvisor.tez.utils;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.MalformedURLException;

import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.FAILED;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.FAILED_DAG;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.KILLED;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.KILLED_DAG;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.SUCCESS;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.SUCCESS_DAG;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TEST_APP_LIST;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TEST_TEZ_CONFIGURE;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TEST_TEZ_QUERY;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TIME_14;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.TIME_18;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.UNFINISHED;
import static com.huawei.boostkit.omniadvisor.tez.utils.TestTezContext.UNFINISHED_DAG;

public class TestJsonUtilsFactory {
    public static TezJsonUtils getAppListJsonUtils() throws AuthenticationException, IOException {
        TezJsonUtils tezJsonUtils = Mockito.mock(TezJsonUtils.class);
        Mockito.when(tezJsonUtils.getApplicationJobs(TIME_14, TIME_18)).thenReturn(TEST_APP_LIST);
        return tezJsonUtils;
    }

    public static TezJsonUtils getSuccessJsonUtils() throws MalformedURLException {
        TezJsonUtils successJsonUtils = Mockito.mock(TezJsonUtils.class);
        Mockito.when(successJsonUtils.getDAGIds(SUCCESS)).thenReturn(ImmutableList.of(SUCCESS_DAG));
        Mockito.when(successJsonUtils.getConfigure(SUCCESS)).thenReturn(TEST_TEZ_CONFIGURE);
        Mockito.when(successJsonUtils.getQueryString(SUCCESS)).thenReturn(TEST_TEZ_QUERY);
        return successJsonUtils;
    }

    public static TezJsonUtils getFailedJsonUtils() throws MalformedURLException {
        TezJsonUtils failedJsonUtils = Mockito.mock(TezJsonUtils.class);
        Mockito.when(failedJsonUtils.getDAGIds(FAILED)).thenReturn(ImmutableList.of(FAILED_DAG));
        Mockito.when(failedJsonUtils.getConfigure(FAILED)).thenReturn(TEST_TEZ_CONFIGURE);
        Mockito.when(failedJsonUtils.getQueryString(FAILED)).thenReturn(TEST_TEZ_QUERY);
        return failedJsonUtils;
    }

    public static TezJsonUtils getKilledJsonUtils() throws MalformedURLException {
        TezJsonUtils killedJsonUtils = Mockito.mock(TezJsonUtils.class);
        Mockito.when(killedJsonUtils.getDAGIds(KILLED)).thenReturn(ImmutableList.of(KILLED_DAG));
        Mockito.when(killedJsonUtils.getConfigure(KILLED)).thenReturn(TEST_TEZ_CONFIGURE);
        Mockito.when(killedJsonUtils.getQueryString(KILLED)).thenReturn(TEST_TEZ_QUERY);
        return killedJsonUtils;
    }

    public static TezJsonUtils getUnFinishedJsonUtils() throws MalformedURLException {
        TezJsonUtils unFinishedJsonUtils = Mockito.mock(TezJsonUtils.class);
        Mockito.when(unFinishedJsonUtils.getDAGIds(UNFINISHED)).thenReturn(ImmutableList.of(UNFINISHED_DAG));
        Mockito.when(unFinishedJsonUtils.getConfigure(UNFINISHED)).thenReturn(TEST_TEZ_CONFIGURE);
        Mockito.when(unFinishedJsonUtils.getQueryString(UNFINISHED)).thenReturn(TEST_TEZ_QUERY);
        return unFinishedJsonUtils;
    }
}
