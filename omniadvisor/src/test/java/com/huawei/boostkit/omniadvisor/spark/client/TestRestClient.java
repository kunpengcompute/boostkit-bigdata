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

package com.huawei.boostkit.omniadvisor.spark.client;

import com.huawei.boostkit.omniadvisor.analysis.AnalyticJob;
import com.huawei.boostkit.omniadvisor.configuration.BaseTestConfiguration;
import com.huawei.boostkit.omniadvisor.dto.ApplicationResult;
import com.huawei.boostkit.omniadvisor.fetcher.FetcherType;
import com.huawei.boostkit.omniadvisor.models.AppResult;
import com.huawei.boostkit.omniadvisor.spark.data.SparkRestAnalyticJob;
import org.apache.spark.SparkConf;
import org.junit.Test;
import org.mockito.Mockito;
import scala.collection.immutable.List;

import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class TestRestClient extends BaseTestConfiguration {
    private static final String TEST_URL = "http://testUri";
    private static final String TEST_APP_INFO =
            "{" +
                    "\"id\" : \"test\"," +
                    "\"name\" : \"test\"," +
                    "\"attempts\" : [{" +
                    "\"startTime\" : \"2023-09-08T03:10:30.194GMT\"," +
                    "\"endTime\" : \"2023-09-08T03:11:34.864GMT\"," +
                    "\"lastUpdated\" : \"2023-09-08T03:11:34.970GMT\"," +
                    "\"duration\" : 62670," +
                    "\"sparkUser\" : \"root\"," +
                    "\"completed\" : true," +
                    "\"appSparkVersion\" : \"3.1.1\"," +
                    "\"startTimeEpoch\" : 1694142632194," +
                    "\"lastUpdatedEpoch\" : 1694142694970," +
                    "\"endTimeEpoch\" : 1694142694864" +
                    "}]" +
                    "}";

    private static final String TEST_APP_INFO_LIST = "[" + TEST_APP_INFO + "]";

    private static final String TEST_EMPTY_APP_INFO_LIST = "[{\"id\":\"test\",\"name\":\"test\",\"attempts\":[]}]";

    @Test
    public void testGetApplications() throws URISyntaxException {
        SparkRestClient restClient = new SparkRestClient("history-url", 1, new SparkConf(), "default");
        WebTarget webTarget = Mockito.mock(WebTarget.class);
        when(webTarget.getUri()).thenReturn(new URI(TEST_URL));
        when(webTarget.path(any())).thenReturn(webTarget);
        when(webTarget.queryParam(any(), any())).thenReturn(webTarget);
        Builder builder = Mockito.mock(Builder.class);
        when(builder.get(String.class)).thenReturn(TEST_APP_INFO_LIST);
        when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        restClient.setApiTarget(webTarget);
        List<AnalyticJob> jobList = restClient.fetchAnalyticJobs(0L, 100L, null);
        assertEquals(jobList.size(), 1);
    }

    @Test
    public void testGetEmptyApplication() throws URISyntaxException {
        SparkRestClient restClient = new SparkRestClient("history-url", 1, new SparkConf(), "default");
        WebTarget webTarget = Mockito.mock(WebTarget.class);
        when(webTarget.getUri()).thenReturn(new URI(TEST_URL));
        when(webTarget.path(any())).thenReturn(webTarget);
        when(webTarget.queryParam(any(), any())).thenReturn(webTarget);
        Builder builder = Mockito.mock(Builder.class);
        when(builder.get(String.class)).thenReturn(TEST_EMPTY_APP_INFO_LIST);
        when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        restClient.setApiTarget(webTarget);
        List<AnalyticJob> jobList = restClient.fetchAnalyticJobs(0L, 100L, null);
        assertEquals(jobList.size(), 0);
    }

    @Test
    public void testAnalysis() throws IOException, URISyntaxException {
        // build test file
        final File workDir = new File(System.getProperty("test.dir", "target"));

        URL filePath = Thread.currentThread().getContextClassLoader()
                .getResource("spark-events/application_1516285256255_0012");
        assertNotNull(filePath);
        File fileToZip = new File(filePath.getPath());

        String outZip = workDir + System.getProperty("file.separator") + "output.zip";
        File outputFile = new File(outZip);

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             ZipOutputStream zos = new ZipOutputStream(fos)) {

            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zos.putNextEntry(zipEntry);

            try (FileInputStream fis = new FileInputStream(fileToZip)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
            }
        }

        // test analyze
        SparkRestAnalyticJob restJob = new SparkRestAnalyticJob("application_1516285256255_0012");
        assertEquals(restJob.getType(), FetcherType.SPARK);
        SparkRestClient restClient = new SparkRestClient("history-url", 1, new SparkConf(), "default");
        WebTarget webTarget = Mockito.mock(WebTarget.class);
        when(webTarget.getUri()).thenReturn(new URI(TEST_URL));
        when(webTarget.path(any())).thenReturn(webTarget);
        when(webTarget.queryParam(any(), any())).thenReturn(webTarget);
        Builder builder = Mockito.mock(Builder.class);
        InputStream inputStream = new FileInputStream(outputFile.getAbsoluteFile());
        when(builder.get(InputStream.class)).thenReturn(inputStream);
        when(builder.get(String.class)).thenReturn(TEST_APP_INFO);
        when(webTarget.request(MediaType.APPLICATION_OCTET_STREAM)).thenReturn(builder);
        when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        restClient.setApiTarget(webTarget);
        ApplicationResult result = restClient.fetchAnalyticResult(restJob);
        assertEquals(result.getApplicationId(), "application_1516285256255_0012");
    }
}
