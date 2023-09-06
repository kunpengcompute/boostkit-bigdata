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

package com.huawei.boostkit.omnituning.tez.data;

import com.huawei.boostkit.omnituning.spark.data.SparkRestAnalyticJob;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.tez.dag.app.dag.DAGState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTezData {
    @Test
    public void testTezAnalyticJobEquals() {
        TezAnalyticJob job1 = new TezAnalyticJob("id", "name", 0L, 1L, YarnApplicationState.RUNNING);
        TezAnalyticJob job2 = new TezAnalyticJob("id", "name", 0L, 1L, YarnApplicationState.RUNNING);
        TezAnalyticJob job3 = new TezAnalyticJob("no", "nn", 0L, 1L, YarnApplicationState.RUNNING);
        SparkRestAnalyticJob restJob = new SparkRestAnalyticJob("id");

        assertTrue(job1.equals(job1));
        assertTrue(job1.equals(job2));
        assertFalse(job1.equals(job3));
        assertFalse(job1.equals(restJob));
    }

    @Test
    public void testTezDagIdEquals() {
        TezDagIdData data1 = new TezDagIdData("id", 0L, 1L, 1L, DAGState.SUCCEEDED);
        TezDagIdData data2 = new TezDagIdData("id", 0L, 1L, 1L, DAGState.SUCCEEDED);
        TezDagIdData data3 = new TezDagIdData("id2", 0L, 1L, 1L, DAGState.SUCCEEDED);
        TezAnalyticJob job = new TezAnalyticJob("id", "name", 0L, 1L, YarnApplicationState.RUNNING);

        assertEquals(data1, data1);
        assertEquals(data1, data2);
        assertFalse(data1.equals(data3));
        assertFalse(data1.equals(job));
    }

    @Test
    public void testTezDatIdCompare() {
        TezDagIdData data1 = new TezDagIdData("id1", 0L, 1L, 1L, DAGState.SUCCEEDED);
        TezDagIdData data2 = new TezDagIdData("id2", 0L, 2L, 2L, DAGState.SUCCEEDED);
        assertEquals(0, data1.compareTo(data2));
    }
}
