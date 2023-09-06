package com.huawei.boostkit.omnituning.tez.utils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;

public class TestUrlFactory {
    private static final String BASE_URL = "http://localhost:8088";
    private static TezUrlFactory urlFactory;

    @BeforeClass
    public static void setUpClass() {
        urlFactory = new TezUrlFactory(BASE_URL);
    }

    @Test
    public void testGetRootURL() throws MalformedURLException {
        assertEquals(urlFactory.getRootURL().toString(), "http://localhost:8088/ws/v1/timeline");
    }

    @Test
    public void testGetApplicationURL() throws MalformedURLException {
        assertEquals(urlFactory.getApplicationURL("appId").toString(),
                "http://localhost:8088/ws/v1/timeline/TEZ_APPLICATION/tez_appId");
    }

    @Test
    public void testGetDagIdURL() throws MalformedURLException {
        assertEquals(urlFactory.getDagIdURL("appId").toString(),
                "http://localhost:8088/ws/v1/timeline/TEZ_DAG_ID?primaryFilter=applicationId:appId");
    }

    @Test
    public void testGetDagExtraInfoURL() throws MalformedURLException {
        assertEquals(urlFactory.getDagExtraInfoURL("dagId").toString(),
                "http://localhost:8088/ws/v1/timeline/TEZ_DAG_EXTRA_INFO/dagId");
    }

    @Test
    public void testGetApplicationHistoryURL() throws MalformedURLException {
        assertEquals(urlFactory.getApplicationHistoryURL(0L, 1L).toString(),
                "http://localhost:8088/ws/v1/applicationhistory/apps?applicationTypes=TEZ&startedTimeBegin=0&startedTimeEnd=1");
    }
}
