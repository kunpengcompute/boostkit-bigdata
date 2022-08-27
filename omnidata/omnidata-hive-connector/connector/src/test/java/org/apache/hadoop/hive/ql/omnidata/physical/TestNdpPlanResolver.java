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

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.status.NdpStatusInfo;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * TestNdpPlanResolver
 *
 * @since 2022-08-24
 */
public class TestNdpPlanResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TestNdpPlanResolver.class);

    private static TestingServer testZkServer;

    private static CuratorFramework testZkClient;

    private static HiveConf testConf;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static class UnsupportedFunc extends ExprNodeDesc {
        @Override
        public List<ExprNodeDesc> getChildren() {
            return new ArrayList<>();
        }

        @Override
        public String toString() {
            return "UnSupportFunc";
        }

        @Override
        public ExprNodeDesc clone() {
            return null;
        }

        @Override
        public boolean isSame(Object obj) {
            return false;
        }
    }

    @BeforeClass
    public static void setUpTestZkServer() throws Exception {
        testZkServer = new TestingServer();

        testConf = new HiveConf();
        testConf.set(OmniDataConf.OMNIDATA_HIVE_ENABLED, "true");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY_ENABLED, "false");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_QUORUM_SERVER, testZkServer.getConnectString());
        testConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_SECURITY_ENABLED, "false");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_FILTER_SELECTIVITY, "1.0");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_TABLE_SIZE_THRESHOLD, "1");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_STATUS_NODE, "/sdi/status");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_ZOOKEEPER_CONF_PATH, "/usr/local/zookeeper/conf");
        testConf.set(OmniDataConf.OMNIDATA_HIVE_REDUCE_OPTIMIZED_ENABLED, "true");
        testZkClient = CuratorFrameworkFactory.builder()
                .connectString(testZkServer.getConnectString())
                .sessionTimeoutMs(OmniDataConf.getOmniDataZookeeperSessionTimeout(testConf))
                .connectionTimeoutMs(OmniDataConf.getOmniDataZookeeperConnectionTimeout(testConf))
                .retryPolicy(new RetryForever(OmniDataConf.getOmniDataZookeeperRetryInterval(testConf)))
                .build();
        testZkClient.start();
        String testNode = "/sdi/status/testNode";

        NdpStatusInfo statusInfo = new NdpStatusInfo("testNode", "testVersion", 0.8, 0, 1000);

        byte[] data = MAPPER.writeValueAsBytes(statusInfo);

        // create a test node in zk for unit test
        testZkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(testNode);

        testZkClient.setData().forPath(testNode, data);
    }

    @AfterClass
    public static void closeTestZkServer() {
        try {
            testZkClient.close();
            testZkServer.close();
        } catch (IOException e) {
            // ignore it
        }
    }

    /**
     * use Mock to create TableScanOperator
     *
     * @return TableScanOperator
     */
    public static TableScanOperator createTestTableScanOperator() {
        // build tableScanOperator
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        String schemaEvolutionColumns = "col1,col2,col3";
        String schemaEvolutionColumnsTypes = "int,int,int";
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
        return tableScanOperator;
    }

    @Test
    public void testResolveTezWorkWithAgg() {
        PhysicalContext pctx = mock(PhysicalContext.class);

        MapWork baseWork = mock(MapWork.class);
        when(baseWork.getName()).thenReturn("test");
        VectorizedRowBatchCtx vectorizedRowBatchCtx = new VectorizedRowBatchCtx(new String[] {}, new TypeInfo[] {},
                new DataTypePhysicalVariation[] {}, new int[] {}, 0, 0, new VirtualColumn[] {}, new String[] {},
                new DataTypePhysicalVariation[] {});
        when(baseWork.getVectorizedRowBatchCtx()).thenReturn(vectorizedRowBatchCtx);
        when(pctx.getConf()).thenReturn(testConf);

        // build tableScan Operator
        TableScanOperator tableScanOperator = createTestTableScanOperator();

        List<Operator<? extends OperatorDesc>> tableScanChildren = new ArrayList<>();
        // build filterOperator
        VectorFilterOperator filterOperator = mock(VectorFilterOperator.class);
        tableScanChildren.add(filterOperator);
        // set child
        when(tableScanOperator.getChildOperators()).thenReturn(tableScanChildren);

        // build exprNodeDesc
        List<ExprNodeDesc> exprChildren = new ArrayList<>();
        exprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        exprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));

        ExprNodeGenericFuncDesc predicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(), "=",
                exprChildren);
        Statistics filterStatistics = new Statistics(10L, 0L);
        FilterDesc filterDesc = new FilterDesc(predicate, false);
        filterDesc.setStatistics(filterStatistics);

        when(filterOperator.getConf()).thenReturn(filterDesc);

        // build selectOp
        VectorSelectOperator selectOperator = mock(VectorSelectOperator.class);
        List<Operator<? extends OperatorDesc>> filterChildren = new ArrayList<>();
        filterChildren.add(selectOperator);
        List<Operator<? extends OperatorDesc>> filterParent = new ArrayList<>();
        filterParent.add(tableScanOperator);
        when(filterOperator.getNumChild()).thenReturn(1);
        when(filterOperator.getChildOperators()).thenReturn(filterChildren);
        when(filterOperator.getNumParent()).thenReturn(1);
        when(filterOperator.getParentOperators()).thenReturn(filterParent);

        // build vectorSelectDesc
        VectorSelectDesc vSelectDesc = mock(VectorSelectDesc.class);
        when(vSelectDesc.getSelectExpressions()).thenReturn(new VectorExpression[] {});
        SelectDesc selectDesc = mock(SelectDesc.class);
        when(selectDesc.getVectorDesc()).thenReturn(vSelectDesc);
        when(selectOperator.getConf()).thenReturn(selectDesc);

        // build groupByOperator
        VectorGroupByOperator groupByOperator = mock(VectorGroupByOperator.class);
        List<Operator<? extends OperatorDesc>> selectChildren = new ArrayList<>();
        List<Operator<? extends OperatorDesc>> selectParent = new ArrayList<>();
        when(selectOperator.getNumChild()).thenReturn(1);
        when(selectOperator.getNumParent()).thenReturn(1);
        selectChildren.add(groupByOperator);
        selectParent.add(filterOperator);
        when(selectOperator.getChildOperators()).thenReturn(selectChildren);
        when(selectOperator.getParentOperators()).thenReturn(selectParent);

        // build groupByDesc
        IdentityExpression keyExpression = mock(IdentityExpression.class);
        when(keyExpression.getOutputColumnNum()).thenReturn(2);
        when(keyExpression.getOutputTypeInfo()).thenReturn(intTypeInfo);

        VectorAggregationDesc countAggregationDesc = mock(VectorAggregationDesc.class);
        AggregationDesc countAggDesc = mock(AggregationDesc.class);
        when(countAggDesc.getGenericUDAFName()).thenReturn("count");
        when(countAggDesc.getDistinct()).thenReturn(false);
        when(countAggregationDesc.getAggrDesc()).thenReturn(countAggDesc);
        when(countAggregationDesc.getInputTypeInfo()).thenReturn(intTypeInfo);
        IdentityExpression countAggInputExpression = mock(IdentityExpression.class);
        when(countAggInputExpression.getOutputColumnNum()).thenReturn(1);
        when(countAggInputExpression.getOutputTypeInfo()).thenReturn(intTypeInfo);
        when(countAggregationDesc.getInputExpression()).thenReturn(countAggInputExpression);
        when(countAggregationDesc.getOutputTypeInfo()).thenReturn(longTypeInfo);

        VectorAggregationDesc minAggregationDesc = mock(VectorAggregationDesc.class);
        AggregationDesc minAggDesc = mock(AggregationDesc.class);
        when(minAggDesc.getGenericUDAFName()).thenReturn("min");
        when(minAggDesc.getDistinct()).thenReturn(false);
        ArrayList<ExprNodeDesc> minParameters = new ArrayList<>();
        ExprNodeDesc minExprNodeColumnDesc = new ExprNodeColumnDesc(intTypeInfo, "col2", "test", false);
        minParameters.add(minExprNodeColumnDesc);
        when(minAggDesc.getParameters()).thenReturn(minParameters);
        when(minAggregationDesc.getAggrDesc()).thenReturn(minAggDesc);
        when(minAggregationDesc.getInputTypeInfo()).thenReturn(intTypeInfo);
        IdentityExpression minAggInputExpression = mock(IdentityExpression.class);
        when(minAggInputExpression.getOutputColumnNum()).thenReturn(1);
        when(minAggInputExpression.getOutputTypeInfo()).thenReturn(intTypeInfo);
        when(minAggregationDesc.getInputExpression()).thenReturn(minAggInputExpression);
        when(minAggregationDesc.getOutputTypeInfo()).thenReturn(intTypeInfo);

        VectorAggregationDesc sumAggregationDesc = mock(VectorAggregationDesc.class);
        AggregationDesc sumAggDesc = mock(AggregationDesc.class);
        when(sumAggDesc.getGenericUDAFName()).thenReturn("sum");
        when(sumAggDesc.getDistinct()).thenReturn(false);
        ArrayList<ExprNodeDesc> sumParameters = new ArrayList<>();
        ExprNodeDesc sumExprNodeColumnDesc = new ExprNodeColumnDesc(intTypeInfo, "col2", "test", false);
        sumParameters.add(sumExprNodeColumnDesc);
        when(minAggDesc.getParameters()).thenReturn(sumParameters);
        when(sumAggregationDesc.getAggrDesc()).thenReturn(sumAggDesc);
        when(sumAggregationDesc.getInputTypeInfo()).thenReturn(intTypeInfo);
        IdentityExpression sumAggInputExpression = mock(IdentityExpression.class);
        when(sumAggInputExpression.getOutputColumnNum()).thenReturn(1);
        when(sumAggInputExpression.getOutputTypeInfo()).thenReturn(intTypeInfo);
        when(sumAggregationDesc.getInputExpression()).thenReturn(sumAggInputExpression);
        when(sumAggregationDesc.getOutputTypeInfo()).thenReturn(longTypeInfo);

        VectorGroupByDesc vGroupByDesc = mock(VectorGroupByDesc.class);
        when(vGroupByDesc.getKeyExpressions()).thenReturn(new VectorExpression[] {keyExpression});
        when(vGroupByDesc.getVecAggrDescs()).thenReturn(
                new VectorAggregationDesc[] {countAggregationDesc, minAggregationDesc});
        GroupByDesc groupByDesc = mock(GroupByDesc.class);
        when(groupByDesc.getVectorDesc()).thenReturn(vGroupByDesc);
        when(groupByOperator.getConf()).thenReturn(groupByDesc);
        when(groupByOperator.getVectorDesc()).thenReturn(vGroupByDesc);

        // build limit operator
        VectorLimitOperator limitOperator = mock(VectorLimitOperator.class);
        LimitDesc limitDesc = mock(LimitDesc.class);
        when(limitDesc.getLimit()).thenReturn(1);
        when(limitDesc.getOffset()).thenReturn(null);
        when(limitOperator.getConf()).thenReturn(limitDesc);
        List<Operator<? extends OperatorDesc>> groupByChildren = new ArrayList<>();
        List<Operator<? extends OperatorDesc>> groupByParent = new ArrayList<>();
        List<Operator<? extends OperatorDesc>> limitParent = new ArrayList<>();
        when(groupByOperator.getNumChild()).thenReturn(1);
        when(groupByOperator.getNumParent()).thenReturn(1);
        when(limitOperator.getNumParent()).thenReturn(1);
        groupByChildren.add(limitOperator);
        groupByParent.add(selectOperator);
        limitParent.add(groupByOperator);
        when(groupByOperator.getChildOperators()).thenReturn(groupByChildren);
        when(groupByOperator.getParentOperators()).thenReturn(groupByParent);
        when(limitOperator.getParentOperators()).thenReturn(limitParent);

        LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<>();
        aliasToWork.put("test", tableScanOperator);
        when(baseWork.getAliasToWork()).thenReturn(aliasToWork);

        List<BaseWork> works = new ArrayList<>();
        works.add(baseWork);
        TezWork tezWork = mock(TezWork.class);
        when(tezWork.getAllWork()).thenReturn(works);

        Task testTask = new Task() {
            @Override
            protected int execute(DriverContext driverContext) {
                return 0;
            }

            @Override
            public StageType getType() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }
        };
        Task task = mock(testTask.getClass());
        when(task.isMapRedTask()).thenReturn(true);
        when(task.isMapRedLocalTask()).thenReturn(true);
        when(task.getWork()).thenReturn(tezWork);
        List<Task<? extends Serializable>> rootTasks = new ArrayList<>();
        rootTasks.add(task);
        when(pctx.getRootTasks()).thenReturn(rootTasks);
        Context context = mock(Context.class);
        when(context.getCmd()).thenReturn("aaa");
        when(pctx.getContext()).thenReturn(context);
        try {
            NdpPlanResolver ndpPlanResolver = new NdpPlanResolver();
            ndpPlanResolver.resolve(pctx);
            assertTrue(tableScanOperator.getConf().isPushDownFilter());
            assertTrue(tableScanOperator.getConf().isPushDownAgg());
        } catch (SemanticException e) {
            LOG.error("OmniData Hive TestNdpPlanResolver testResolveTezWorkWithAgg() failed ", e);
        }
    }

    @Test
    public void testResolveTezWork() {
        PhysicalContext pctx = mock(PhysicalContext.class);

        MapWork baseWork = mock(MapWork.class);
        when(baseWork.getName()).thenReturn("test");
        VectorizedRowBatchCtx vectorizedRowBatchCtx = new VectorizedRowBatchCtx(new String[] {}, new TypeInfo[] {},
                new DataTypePhysicalVariation[] {}, new int[] {}, 0, 0, new VirtualColumn[] {}, new String[] {},
                new DataTypePhysicalVariation[] {});
        when(baseWork.getVectorizedRowBatchCtx()).thenReturn(vectorizedRowBatchCtx);
        when(pctx.getConf()).thenReturn(testConf);

        // build tableScan Operator
        TableScanOperator tableScanOperator = createTestTableScanOperator();

        List<Operator<? extends OperatorDesc>> tableScanChildren = new ArrayList<>();

        // build filterOperator
        VectorFilterOperator filterOperator = mock(VectorFilterOperator.class);
        tableScanChildren.add(filterOperator);
        when(tableScanOperator.getChildOperators()).thenReturn(tableScanChildren);

        // build exprNodeDesc
        List<ExprNodeDesc> exprChildren = new ArrayList<>();
        exprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        exprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));

        ExprNodeGenericFuncDesc predicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(), "=",
                exprChildren);
        Statistics filterStatistics = new Statistics(10L, 0L);
        FilterDesc filterDesc = new FilterDesc(predicate, false);
        filterDesc.setStatistics(filterStatistics);

        when(filterOperator.getConf()).thenReturn(filterDesc);

        // build selectOp
        VectorSelectOperator selectOperator = mock(VectorSelectOperator.class);
        List<Operator<? extends OperatorDesc>> filterChildren = new ArrayList<>();
        filterChildren.add(selectOperator);
        List<Operator<? extends OperatorDesc>> filterParent = new ArrayList<>();
        filterParent.add(tableScanOperator);
        when(filterOperator.getNumChild()).thenReturn(1);
        when(filterOperator.getChildOperators()).thenReturn(filterChildren);
        when(filterOperator.getNumParent()).thenReturn(1);
        when(filterOperator.getParentOperators()).thenReturn(filterParent);

        // build vectorSelectDesc
        VectorSelectDesc vSelectDesc = mock(VectorSelectDesc.class);
        when(vSelectDesc.getSelectExpressions()).thenReturn(new VectorExpression[] {});
        SelectDesc selectDesc = mock(SelectDesc.class);
        when(selectDesc.getVectorDesc()).thenReturn(vSelectDesc);
        when(selectOperator.getConf()).thenReturn(selectDesc);

        // build limit operator
        VectorLimitOperator limitOperator = mock(VectorLimitOperator.class);
        LimitDesc limitDesc = mock(LimitDesc.class);
        when(limitDesc.getLimit()).thenReturn(7);
        when(limitDesc.getOffset()).thenReturn(null);
        when(limitOperator.getConf()).thenReturn(limitDesc);
        List<Operator<? extends OperatorDesc>> limitParent = new ArrayList<>();
        List<Operator<? extends OperatorDesc>> selectChildren = new ArrayList<>();
        List<Operator<? extends OperatorDesc>> selectParent = new ArrayList<>();
        when(selectOperator.getNumChild()).thenReturn(1);
        when(selectOperator.getNumParent()).thenReturn(1);
        selectChildren.add(limitOperator);
        selectParent.add(filterOperator);
        when(selectOperator.getChildOperators()).thenReturn(selectChildren);
        when(selectOperator.getParentOperators()).thenReturn(selectParent);
        when(limitOperator.getNumParent()).thenReturn(1);
        limitParent.add(selectOperator);
        when(limitOperator.getParentOperators()).thenReturn(limitParent);

        LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<>();
        aliasToWork.put("test", tableScanOperator);
        when(baseWork.getAliasToWork()).thenReturn(aliasToWork);

        List<BaseWork> works = new ArrayList<>();
        works.add(baseWork);
        TezWork tezWork = mock(TezWork.class);
        when(tezWork.getAllWork()).thenReturn(works);

        Task testTask = new Task() {
            @Override
            protected int execute(DriverContext driverContext) {
                return 0;
            }

            @Override
            public StageType getType() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }
        };
        Task task = mock(testTask.getClass());
        when(task.isMapRedTask()).thenReturn(true);
        when(task.isMapRedLocalTask()).thenReturn(true);
        when(task.getWork()).thenReturn(tezWork);
        List<Task<? extends Serializable>> rootTasks = new ArrayList<>();
        rootTasks.add(task);
        when(pctx.getRootTasks()).thenReturn(rootTasks);
        Context context = mock(Context.class);
        when(context.getCmd()).thenReturn("aaa");
        when(pctx.getContext()).thenReturn(context);
        try {
            NdpPlanResolver ndpPlanResolver = new NdpPlanResolver();
            ndpPlanResolver.resolve(pctx);
            assertTrue(tableScanOperator.getConf().isPushDownFilter());
            assertFalse(tableScanOperator.getConf().isPushDownAgg());
        } catch (SemanticException e) {
            LOG.error("OmniData Hive TestNdpPlanResolver testResolveTezWork() failed ", e);
        }
    }
}