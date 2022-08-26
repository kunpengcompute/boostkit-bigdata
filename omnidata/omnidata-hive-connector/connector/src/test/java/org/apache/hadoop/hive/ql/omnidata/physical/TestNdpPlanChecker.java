/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.physical;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusInfo;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for NdpPlanChecker
 *
 * @since 2022-08-24
 */
public class TestNdpPlanChecker {
    @Test
    public void testCheckRollUp() {
        String cmd = " roll up( ";
        Assert.assertFalse(NdpPlanChecker.checkRollUp(cmd));
    }

    @Test
    public void testCheckTableScanNumChild() {
        TableScanOperator tableScanOp = TestNdpPlanResolver.createTestTableScanOperator();
        when(tableScanOp.getNumChild()).thenReturn(2);
        Assert.assertFalse(NdpPlanChecker.checkTableScanNumChild(tableScanOp));
    }

    @Test
    public void testCheckHostResources() {
        Map<String, NdpStatusInfo> ndpStatusInfoMap1 = new HashMap<>();
        Assert.assertFalse(NdpPlanChecker.checkHostResources(ndpStatusInfoMap1));

        Map<String, NdpStatusInfo> ndpStatusInfoMap2 = new HashMap<>();
        ndpStatusInfoMap2.put("agent1", new NdpStatusInfo("agent1", "1.0.0", 0.8, 5, 10));
        ndpStatusInfoMap2.put("agent2", new NdpStatusInfo("agent2", "1.0.0", 0.8, 5, 10));
        ndpStatusInfoMap2.put("agent3", new NdpStatusInfo("agent3", "1.0.0", 0.8, 10, 10));
        Assert.assertTrue(NdpPlanChecker.checkHostResources(ndpStatusInfoMap2));
        Assert.assertEquals(ndpStatusInfoMap2.size(), 2);

        Map<String, NdpStatusInfo> ndpStatusInfoMap3 = new HashMap<>();
        ndpStatusInfoMap3.put("agent1", new NdpStatusInfo("agent1", "1.0.0", 0.8, 10, 10));
        ndpStatusInfoMap3.put("agent2", new NdpStatusInfo("agent2", "1.0.0", 0.8, 10, 10));
        ndpStatusInfoMap3.put("agent3", new NdpStatusInfo("agent3", "1.0.0", 0.8, 10, 10));
        Assert.assertFalse(NdpPlanChecker.checkHostResources(ndpStatusInfoMap3));
        Assert.assertEquals(ndpStatusInfoMap3.size(), 0);
    }

    @Test
    public void testCheckUdfByWhiteList() {
        Assert.assertFalse(NdpPlanChecker.checkUdfByWhiteList(NdpUdfEnum.UNSUPPORTED));
    }

    @Test
    public void testCheckHiveType() {
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        String schemaEvolutionColumns = "col1,col2,col3";
        String schemaEvolutionColumnsTypes = "char(11),binary,int";
        when(tableScanOperator.getNumChild()).thenReturn(1);
        when(tableScanOperator.getSchemaEvolutionColumns()).thenReturn(schemaEvolutionColumns);
        when(tableScanOperator.getSchemaEvolutionColumnsTypes()).thenReturn(schemaEvolutionColumnsTypes);

        // build tableScanDesc
        Table table = new Table("test", "test");
        table.setInputFormatClass(OrcInputFormat.class);
        table.setOutputFormatClass(OrcOutputFormat.class);
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        List<Integer> neededColumnIDs = new ArrayList<>();
        neededColumnIDs.add(0);
        neededColumnIDs.add(1);
        neededColumnIDs.add(2);
        List<String> neededColumns = new ArrayList<>();
        neededColumns.add("col1");
        neededColumns.add("col2");
        neededColumns.add("col3");
        Statistics tableScanStatistics = new Statistics(1000L, 100L);
        tableScanDesc.setStatistics(tableScanStatistics);
        tableScanDesc.setNeededColumnIDs(neededColumnIDs);
        tableScanDesc.setNeededColumns(neededColumns);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);
        Assert.assertFalse(NdpPlanChecker.checkHiveType(tableScanOperator));
    }

    @Test
    public void testCheckOperator() {
        Assert.assertFalse(NdpPlanChecker.checkFilterOperator(null).isPresent());
        Assert.assertFalse(NdpPlanChecker.checkSelectOperator(null).isPresent());
        Assert.assertFalse(NdpPlanChecker.checkGroupByOperator(null).isPresent());
        Assert.assertFalse(NdpPlanChecker.checkLimitOperator(null).isPresent());
    }

    @Test
    public void testCheckCountAgg() {
        // count
        AggregationDesc count = mock(AggregationDesc.class);
        when(count.getGenericUDAFName()).thenReturn("count");

        // count: return true
        ArrayList<ExprNodeDesc> count1 = new ArrayList<>();
        count1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(count.getParameters()).thenReturn(count1);
        Assert.assertTrue(NdpPlanChecker.checkAggregationDesc(count));

        // count: return false
        ArrayList<ExprNodeDesc> count2 = new ArrayList<>();
        count2.add(new ExprNodeColumnDesc(binaryTypeInfo, "col1", "test", false));
        when(count.getParameters()).thenReturn(count2);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(count));
    }

    @Test
    public void testCheckAvgAgg() {
        // avg
        AggregationDesc avg = mock(AggregationDesc.class);
        when(avg.getGenericUDAFName()).thenReturn("avg");

        // avg: return true
        ArrayList<ExprNodeDesc> avg1 = new ArrayList<>();
        avg1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(avg.getParameters()).thenReturn(avg1);
        Assert.assertTrue(NdpPlanChecker.checkAggregationDesc(avg));

        // avg: return false
        ArrayList<ExprNodeDesc> avg2 = new ArrayList<>();
        avg2.add(new ExprNodeColumnDesc(binaryTypeInfo, "col1", "test", false));
        when(avg.getParameters()).thenReturn(avg2);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(avg));
    }

    @Test
    public void testCheckSumAgg() {
        // sum
        AggregationDesc sum = mock(AggregationDesc.class);
        when(sum.getGenericUDAFName()).thenReturn("sum");

        // sum: return true
        ArrayList<ExprNodeDesc> sum1 = new ArrayList<>();
        sum1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(sum.getParameters()).thenReturn(sum1);
        Assert.assertTrue(NdpPlanChecker.checkAggregationDesc(sum));

        // sum: return false
        ArrayList<ExprNodeDesc> sum2 = new ArrayList<>();
        sum2.add(new ExprNodeColumnDesc(binaryTypeInfo, "col1", "test", false));
        when(sum.getParameters()).thenReturn(sum2);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(sum));

        // sum distinct: return false
        when(sum.getDistinct()).thenReturn(true);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(sum));
    }

    @Test
    public void testCheckMinAgg() {
        // min
        AggregationDesc min = mock(AggregationDesc.class);
        when(min.getGenericUDAFName()).thenReturn("min");

        // min: return true
        ArrayList<ExprNodeDesc> min1 = new ArrayList<>();
        min1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(min.getParameters()).thenReturn(min1);
        Assert.assertTrue(NdpPlanChecker.checkAggregationDesc(min));

        // min: return false
        ArrayList<ExprNodeDesc> min2 = new ArrayList<>();
        min2.add(new ExprNodeColumnDesc(binaryTypeInfo, "col1", "test", false));
        when(min.getParameters()).thenReturn(min2);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(min));

        // min distinct: return false
        when(min.getDistinct()).thenReturn(true);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(min));
    }

    @Test
    public void testCheckMaxAgg() {
        // max
        AggregationDesc max = mock(AggregationDesc.class);
        when(max.getGenericUDAFName()).thenReturn("max");

        // max: return true
        ArrayList<ExprNodeDesc> max1 = new ArrayList<>();
        max1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(max.getParameters()).thenReturn(max1);
        Assert.assertTrue(NdpPlanChecker.checkAggregationDesc(max));

        // max: return false
        ArrayList<ExprNodeDesc> max2 = new ArrayList<>();
        max2.add(new ExprNodeColumnDesc(binaryTypeInfo, "col1", "test", false));
        when(max.getParameters()).thenReturn(max2);
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(max));

        // unsupported
        AggregationDesc agg = mock(AggregationDesc.class);
        when(agg.getGenericUDAFName()).thenReturn("leaf");
        Assert.assertFalse(NdpPlanChecker.checkAggregationDesc(agg));
    }

    @Test
    public void testCheckHostResources2() {
        NdpStatusInfo statusInfo = mock(NdpStatusInfo.class);
        when(statusInfo.getMaxTasks()).thenReturn(3000);
        when(statusInfo.getThreshold()).thenReturn(0.8);
        when(statusInfo.getRunningTasks()).thenReturn(3000);
        Assert.assertFalse(NdpPlanChecker.checkHostResources(statusInfo));
    }
}