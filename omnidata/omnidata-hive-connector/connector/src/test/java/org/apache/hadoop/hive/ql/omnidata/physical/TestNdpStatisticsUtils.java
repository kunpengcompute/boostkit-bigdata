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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for OmniData Hive statistics tool
 * Used to collect statistics for optimization
 *
 * @since 2022-08-23
 */
public class TestNdpStatisticsUtils {
    private static final int TEST_MAX_REDUCE = 1009;

    @Test
    public void testEstimateReducersByFileSize() {
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(0, 0, 0), 0);
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(102400, 1024, TEST_MAX_REDUCE), 100);
        assertEquals(NdpStatisticsUtils.estimateReducersByFileSize(10240, 1024, TEST_MAX_REDUCE),
                NdpStatisticsUtils.BASED_REDUCES);
    }

    @Test
    public void testSetOptimizedNumReduceTasks() {
        HiveConf testConf = new HiveConf();
        TezEdgeProperty tezEdgeProperty = new TezEdgeProperty(testConf, TezEdgeProperty.EdgeType.SIMPLE_EDGE, -1);
        ReduceWork reduceWork = new ReduceWork("test");
        reduceWork.setAutoReduceParallelism(true);
        reduceWork.setNumReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setMinReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setMaxReduceTasks(TEST_MAX_REDUCE);
        reduceWork.setEdgePropRef(tezEdgeProperty);
        int optimizedNumReduces = 100;
        NdpStatisticsUtils.setOptimizedNumReduceTasks(reduceWork, optimizedNumReduces, 1024);
        assertEquals(reduceWork.getNumReduceTasks().intValue(), optimizedNumReduces);
        assertEquals(reduceWork.getMinReduceTasks(), optimizedNumReduces / 2);
        assertEquals(reduceWork.getMaxReduceTasks(), optimizedNumReduces);
        assertEquals(tezEdgeProperty.getMinReducer(), optimizedNumReduces / 2);
        assertEquals(tezEdgeProperty.getMaxReducer(), optimizedNumReduces);

        reduceWork.setAutoReduceParallelism(false);
        NdpStatisticsUtils.setOptimizedNumReduceTasks(reduceWork, optimizedNumReduces, 1024);
        assertEquals(reduceWork.getNumReduceTasks().intValue(), optimizedNumReduces);
    }

    @Test
    public void testOptimizeLengthPerGroup() {
        double delta = 0.001d;
        HiveConf testConf = new HiveConf();

        // agg configCoefficient
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.5d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, true);
        testConf.setDouble(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_COEFFICIENT, 10d);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 10d, delta);

        // filter configCoefficient
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 10d, delta);

        testConf.clear();

        // agg
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.5d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 3.0d, delta);

        // filter
        OmniDataConf.setOmniDataTableOptimizedSelectivity(testConf, 0.25d);
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 2.5d, delta);

        // group
        OmniDataConf.setOmniDataAggOptimizedEnabled(testConf, false);
        OmniDataConf.setOmniDataFilterOptimizedEnabled(testConf, false);
        testConf.setBoolean(OmniDataConf.OMNIDATA_HIVE_GROUP_OPTIMIZED_ENABLED, true);
        assertEquals(NdpStatisticsUtils.optimizeLengthPerGroup(testConf), 1.8d, delta);
    }

    @Test
    public void testEvaluatePartFilterSelectivity() throws HiveException {
        // build tableScanOperator
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        VectorExpression ndpExpr = mock(VectorExpression.class);
        VectorizationContext vectorizationContext = mock(VectorizationContext.class);
        when(tableScanOperator.getOutputVectorizationContext()).thenReturn(vectorizationContext);
        when(vectorizationContext.getVectorExpression(any(), any())).thenReturn(ndpExpr);

        // build tableScanDesc
        Table table = new Table("test", "test");
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);
        Statistics tableScanStatistics = new Statistics(1000L, 100L);
        tableScanDesc.setStatistics(tableScanStatistics);
        tableScanDesc.setOmniDataSelectivity(0.2d);

        // build filterOperator
        List<Operator<? extends OperatorDesc>> tableScanChildren = new ArrayList<>();
        VectorFilterOperator filterOperator = mock(VectorFilterOperator.class);
        VectorExpression vectorexpression = mock(VectorExpression.class);
        ExprNodeDesc oldExprDesc = mock(ExprNodeDesc.class);
        FilterDesc filterDesc = new FilterDesc();
        Statistics statistics = new Statistics();
        statistics.setNumRows(1024);
        filterDesc.setStatistics(statistics);
        when(filterOperator.getConf()).thenReturn(filterDesc);
        when(filterOperator.getPredicateExpression()).thenReturn(vectorexpression);
        filterDesc.setPredicate(oldExprDesc);
        tableScanChildren.add(filterOperator);
        when(tableScanOperator.getChildOperators()).thenReturn(tableScanChildren);

        // build newExprDesc
        ExprNodeDesc newExprDesc = mock(ExprNodeDesc.class);

        // build parseContext
        ParseContext parseContext = new ParseContext();
        Context context = mock(Context.class);
        HiveConf hiveConf = new HiveConf();
        parseContext.setContext(context);
        parseContext.setConf(hiveConf);

        // build configuration
        Configuration configuration = new Configuration();

        assertTrue(NdpStatisticsUtils.evaluatePartFilterSelectivity(tableScanOperator, newExprDesc, parseContext,
                configuration));
    }

    @Test
    public void testEstimateOptimizedSize() throws HiveException {
        // build tableScanOperator
        TableScanOperator tableScanOperator = TestNdpPlanResolver.createTestTableScanOperator();
        tableScanOperator.getConf().setOmniDataSelectivity(1.0d);
        tableScanOperator.getConf().setAlias("testTable");

        // build rowSchema
        ColumnInfo columnInfo1 = new ColumnInfo();
        ColumnInfo columnInfo2 = new ColumnInfo();
        columnInfo1.setInternalName("col1");
        columnInfo2.setInternalName("col2");
        columnInfo1.setTypeName("int");
        columnInfo2.setTypeName("int");
        ArrayList<ColumnInfo> signature = new ArrayList<>();
        signature.add(columnInfo1);
        signature.add(columnInfo2);
        RowSchema rowSchema = new RowSchema(signature);
        when(tableScanOperator.getSchema()).thenReturn(rowSchema);

        // build parseContext
        Partition partition = mock(Partition.class);
        Map<String, String> partitionParameters = new HashMap<>();
        partitionParameters.put("numRows", "1024");
        partitionParameters.put("rawDataSize", "10240");
        when(partition.getParameters()).thenReturn(partitionParameters);
        Set<Partition> partitions = new HashSet<>();
        partitions.add(partition);
        HiveConf hiveConf = new HiveConf();
        PrunedPartitionList prunedPartitionList = new PrunedPartitionList(
                tableScanOperator.getConf().getTableMetadata(), partitions, new ArrayList<>(), false);
        ColumnStatsList columnStatsList = new ColumnStatsList();
        ParseContext parseContext = mock(ParseContext.class);
        when(parseContext.getPrunedPartitions(any())).thenReturn(prunedPartitionList);
        when(parseContext.getColStatsCached(prunedPartitionList)).thenReturn(columnStatsList);
        when(parseContext.getConf()).thenReturn(hiveConf);

        // build statistics
        Statistics statistics = new Statistics();
        Map<String, ColStatistics> columnStats = new HashMap<>();
        ColStatistics colStatistics1 = new ColStatistics();
        colStatistics1.setAvgColLen(10.0d);
        columnStats.put("col1", colStatistics1);
        statistics.setColumnStats(columnStats);
        MockedStatic<StatsUtils> statsUtils = mockStatic(StatsUtils.class);
        statsUtils.when(() -> StatsUtils.collectStatistics(hiveConf, prunedPartitionList,
                tableScanOperator.getConf().getTableMetadata(), signature,
                tableScanOperator.getNeededColumns(), columnStatsList, tableScanOperator.getReferencedColumns(),
                true)).thenReturn(statistics);

        assertEquals(NdpStatisticsUtils.estimateOptimizedSize(tableScanOperator, parseContext), 100);

        statsUtils.close();
    }

    @Test
    public void testOptimizeReduceWork() {
        // build tableScanOperatorMap
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        Table table = new Table("test", "test");
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        Statistics tableScanStatistics = new Statistics(1000L, 1024 * 1024 * 1024 * 51L);
        List<String> neededColumns = new ArrayList<>();
        tableScanDesc.setStatistics(tableScanStatistics);
        ArrayList<ColumnInfo> signature = new ArrayList<>();
        RowSchema rowSchema = new RowSchema(signature);
        when(tableScanOperator.getSchema()).thenReturn(rowSchema);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);
        when(tableScanOperator.getNeededColumns()).thenReturn(neededColumns);
        Map<String, TableScanOperator> tableScanOperatorMap = new HashMap<>();
        tableScanOperatorMap.put("tag1", tableScanOperator);

        // build reduceWorkMap
        ReduceWork reduceWork = new ReduceWork("test");
        reduceWork.setAutoReduceParallelism(true);
        reduceWork.setNumReduceTasks(1000);
        Map<Integer, String> tagToInput = new HashMap<Integer, String>();
        tagToInput.put(1, "tag1");
        reduceWork.setTagToInput(tagToInput);
        Map<String, ReduceWork> reduceWorkMap = new HashMap<>();
        reduceWorkMap.put("work1", reduceWork);

        // build hiveConf
        HiveConf hiveConf = new HiveConf();
        TezEdgeProperty tezEdgeProperty = new TezEdgeProperty(hiveConf, TezEdgeProperty.EdgeType.SIMPLE_EDGE, -1);
        reduceWork.setEdgePropRef(tezEdgeProperty);

        // build parseContext
        ParseContext parseContext = new ParseContext();

        MockedStatic<StatsUtils> statsUtils = mockStatic(StatsUtils.class);
        List<ColStatistics> colStats = new ArrayList<>();
        statsUtils.when(() -> StatsUtils.getTableColumnStats(table, signature, neededColumns,
                null)).thenReturn(colStats);

        NdpStatisticsUtils.optimizeReduceWork(tableScanOperatorMap, reduceWorkMap, hiveConf, parseContext);
        assertEquals(reduceWork.getNumReduceTasks().intValue(), 51);
        assertEquals(reduceWork.getMinReduceTasks(), 25);
        assertEquals(reduceWork.getMaxReduceTasks(), 51);

        statsUtils.close();
    }
}