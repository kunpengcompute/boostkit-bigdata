/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package nova.hetu.olk;

import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningScheme;
import nova.hetu.olk.memory.OpenLooKengAllocatorFactory;
import nova.hetu.olk.operator.AggregationOmniOperator;
import nova.hetu.olk.operator.DistinctLimitOmniOperator;
import nova.hetu.olk.operator.HashAggregationOmniOperator;
import nova.hetu.olk.operator.HashBuilderOmniOperator;
import nova.hetu.olk.operator.LimitOmniOperator;
import nova.hetu.olk.operator.LookupJoinOmniOperator;
import nova.hetu.olk.operator.LookupJoinOmniOperators;
import nova.hetu.olk.operator.OrderByOmniOperator;
import nova.hetu.olk.operator.PartitionedOutputOmniOperator;
import nova.hetu.olk.operator.TopNOmniOperator;
import nova.hetu.olk.operator.WindowOmniOperator;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@PrepareForTest({VecAllocator.class,
        OpenLooKengAllocatorFactory.class,
        OmniLocalQueryRunner.class,
        OmniLocalExecutionPlanner.class,
        AggregationOmniOperator.class,
        AggregationOmniOperator.AggregationOmniOperatorFactory.class,
        LookupJoinOmniOperators.class,
        OrderByOmniOperator.class,
        OrderByOmniOperator.OrderByOmniOperatorFactory.class,
        PartitionFunction.class,
        NodePartitioningManager.class
})
@SuppressStaticInitializationFor({"nova.hetu.omniruntime.vector.VecAllocator",
        "nova.hetu.omniruntime.constants.Constant",
        "nova.hetu.omniruntime.operator.OmniOperatorFactory"
})
@PowerMockIgnore("javax.management.*")
public class TestOmniLocalExecutionPlanner
        extends PowerMockTestCase
{
    private static OmniLocalQueryRunner runner;
    private static String sqlSort;
    private static String sqlTopn;
    private static String sqlDistinctLimit;
    private static String sqlAgg;
    private static String sqlHashagg;
    private static String sqlJoin;
    private static String sqlWindow;
    private static String sqlFilter;
    private static String sqlRight;
    private static String sqlFull;

    @BeforeMethod
    public void setUp() throws Exception
    {
        runner = new OmniLocalQueryRunner(TEST_SESSION);
        setAggFunTypes();
        mockSupports();
        setSql();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    @Test
    public void testSql()
    {
        runner.executeWithPlanOnly(sqlSort);
        runner.executeWithPlanOnly(sqlTopn);
        runner.executeWithPlanOnly(sqlDistinctLimit);
        runner.executeWithPlanOnly(sqlAgg);
        runner.executeWithPlanOnly(sqlHashagg);
        runner.executeWithPlanOnly(sqlJoin);
        runner.executeWithPlanOnly(sqlWindow);
        runner.executeWithPlanOnly(sqlFilter);
        runner.executeWithPlanOnly(sqlRight);
        runner.executeWithPlanOnly(sqlFull);
    }

    private void setAggFunTypes() throws IllegalAccessException
    {
        FunctionType functionType = new FunctionType(0);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_SUM").set(FunctionType.class, functionType);
        functionType = new FunctionType(1);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_COUNT_COLUMN").set(FunctionType.class, functionType);
        functionType = new FunctionType(2);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_COUNT_ALL").set(FunctionType.class, functionType);
        functionType = new FunctionType(3);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_AVG").set(FunctionType.class, functionType);
        functionType = new FunctionType(4);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_MAX").set(FunctionType.class, functionType);
        functionType = new FunctionType(5);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_MIN").set(FunctionType.class, functionType);
        functionType = new FunctionType(6);
        MemberModifier.field(FunctionType.class, "OMNI_AGGREGATION_TYPE_DNV").set(FunctionType.class, functionType);
        functionType = new FunctionType(8);
        MemberModifier.field(FunctionType.class, "OMNI_WINDOW_TYPE_ROW_NUMBER").set(FunctionType.class, functionType);
        functionType = new FunctionType(9);
        MemberModifier.field(FunctionType.class, "OMNI_WINDOW_TYPE_RANK").set(FunctionType.class, functionType);
    }

    private void mockSupports() throws Exception
    {
        //mock VecAllocator
        VecAllocator vecAllocator = mock(VecAllocator.class);
        mockStatic(OpenLooKengAllocatorFactory.class);
        when(OpenLooKengAllocatorFactory.create(anyString(), any(OpenLooKengAllocatorFactory.CallBack.class))).thenReturn(vecAllocator);

        //mock AggOmniOperator
        AggregationOmniOperator aggregationOmniOperator = mock(AggregationOmniOperator.class);
        AggregationOmniOperator.AggregationOmniOperatorFactory aggregationOmniOperatorFactory = mock(AggregationOmniOperator.AggregationOmniOperatorFactory.class);
        whenNew(AggregationOmniOperator.AggregationOmniOperatorFactory.class).withAnyArguments().thenReturn(aggregationOmniOperatorFactory);
        when(aggregationOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(aggregationOmniOperator);

        //mock TopNOmniOperator
        TopNOmniOperator topNOmniOperator = mock(TopNOmniOperator.class);
        TopNOmniOperator.TopNOmniOperatorFactory topNOmniOperatorFactory = mock(TopNOmniOperator.TopNOmniOperatorFactory.class);
        whenNew(TopNOmniOperator.TopNOmniOperatorFactory.class).withAnyArguments().thenReturn(topNOmniOperatorFactory);
        when(topNOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(topNOmniOperator);

        //mock HashAggOmniOperator
        HashAggregationOmniOperator hashAggregationOmniOperator = mock(HashAggregationOmniOperator.class);
        HashAggregationOmniOperator.HashAggregationOmniOperatorFactory hashAggregationOmniOperatorFactory = mock(HashAggregationOmniOperator.HashAggregationOmniOperatorFactory.class);
        whenNew(HashAggregationOmniOperator.HashAggregationOmniOperatorFactory.class).withAnyArguments().thenReturn(hashAggregationOmniOperatorFactory);
        when(hashAggregationOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(hashAggregationOmniOperator);

        //mock HashBuilderOmniOperator
        HashBuilderOmniOperator hashBuilderOmniOperator = mock(HashBuilderOmniOperator.class);
        HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory = mock(HashBuilderOmniOperator.HashBuilderOmniOperatorFactory.class);
        whenNew(HashBuilderOmniOperator.HashBuilderOmniOperatorFactory.class).withAnyArguments().thenReturn(hashBuilderOmniOperatorFactory);
        when(hashBuilderOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(hashBuilderOmniOperator);

        //mock LookupJoinOmniOperator
        LookupJoinOmniOperator lookupJoinOmniOperator = mock(LookupJoinOmniOperator.class);
        LookupJoinOmniOperator.LookupJoinOmniOperatorFactory lookupJoinOmniOperatorFactory = mock(LookupJoinOmniOperator.LookupJoinOmniOperatorFactory.class);
        whenNew(LookupJoinOmniOperator.LookupJoinOmniOperatorFactory.class).withAnyArguments().thenReturn(lookupJoinOmniOperatorFactory);
        when(lookupJoinOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(lookupJoinOmniOperator);

        //mock LomitOmniOperator
        LimitOmniOperator limitOmniOperator = mock(LimitOmniOperator.class);
        LimitOmniOperator.LimitOmniOperatorFactory limitOmniOperatorFactory = mock(LimitOmniOperator.LimitOmniOperatorFactory.class);
        whenNew(LimitOmniOperator.LimitOmniOperatorFactory.class).withAnyArguments().thenReturn(limitOmniOperatorFactory);
        when(limitOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(limitOmniOperator);

        //mock WindowOmniOperator
        WindowOmniOperator windowOmniOperator = mock(WindowOmniOperator.class);
        WindowOmniOperator.WindowOmniOperatorFactory windowOmniOperatorFactory = mock(WindowOmniOperator.WindowOmniOperatorFactory.class);
        whenNew(WindowOmniOperator.WindowOmniOperatorFactory.class).withAnyArguments().thenReturn(windowOmniOperatorFactory);
        when(windowOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(windowOmniOperator);

        //mock DistinctLimitOmniOperator
        DistinctLimitOmniOperator distinctLimitOmniOperator = mock(DistinctLimitOmniOperator.class);
        DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory distinctLimitOmniOperatorFactory = mock(DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory.class);
        whenNew(DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory.class).withAnyArguments().thenReturn(distinctLimitOmniOperatorFactory);
        when(distinctLimitOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(distinctLimitOmniOperator);

        //mock PartitionedOutputOmniOperator
        PartitionedOutputOmniOperator partitionedOutputOmniOperator = mock(PartitionedOutputOmniOperator.class);
        OperatorFactory operatorFactory = mock(OperatorFactory.class);
        when(operatorFactory.duplicate()).thenReturn(operatorFactory);
        PartitionedOutputOmniOperator.PartitionedOutputOmniFactory partitionedOutputOmniFactory = mock(PartitionedOutputOmniOperator.PartitionedOutputOmniFactory.class);
        whenNew(PartitionedOutputOmniOperator.PartitionedOutputOmniFactory.class).withAnyArguments().thenReturn(partitionedOutputOmniFactory);
        when(partitionedOutputOmniFactory.createOutputOperator(
                anyInt(),
                any(PlanNodeId.class),
                anyList(),
                any(Function.class),
                any(TaskContext.class))).thenReturn(operatorFactory);
        when(operatorFactory.createOperator(any(DriverContext.class))).thenReturn(partitionedOutputOmniOperator);

        //mock OrderByOmniOperator
        OrderByOmniOperator orderByOmniOperator = mock(OrderByOmniOperator.class);
        OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory = mock(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        mockStatic(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        when(OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory(
                anyInt(),
                any(PlanNodeId.class),
                anyList(),
                anyList(),
                anyList(),
                anyList())).thenReturn(orderByOmniOperatorFactory);
        whenNew(OrderByOmniOperator.OrderByOmniOperatorFactory.class).withAnyArguments().thenReturn(orderByOmniOperatorFactory);
        when(orderByOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(orderByOmniOperator);

        //mock supplier
        Supplier supplier = mock(Supplier.class);
        OmniExpressionCompiler omniExpressionCompiler = mock(OmniExpressionCompiler.class);
        when(omniExpressionCompiler.compilePageProcessor(
                any(Optional.class),
                anyList(),
                any(Optional.class),
                any(OptionalInt.class),
                anyList(),
                any(TaskId.class),
                any(OmniLocalExecutionPlanner.OmniLocalExecutionPlanContext.class)))
                .thenReturn(supplier);
        whenNew(OmniExpressionCompiler.class).withAnyArguments().thenReturn(omniExpressionCompiler);

        //mock PartitionFunction
        PartitionFunction partitionFunction = mock(PartitionFunction.class);
        NodePartitioningManager nodePartitioningManager = mock(NodePartitioningManager.class);
        whenNew(NodePartitioningManager.class).withAnyArguments().thenReturn(nodePartitioningManager);
        when(nodePartitioningManager.getPartitionFunction(any(Session.class), any(PartitioningScheme.class), anyList())).thenReturn(partitionFunction);
    }

    private void setSql()
    {
        sqlSort = "select * from system.information_schema.tables order by table_name";
        sqlTopn = "select distinct table_name from system.information_schema.tables order by table_name limit 100";
        sqlDistinctLimit = "select distinct table_name from system.information_schema.tables limit 100";
        sqlAgg = "select sum(ordinal_position),min(ordinal_position),max(ordinal_position),avg(ordinal_position),count(*) from system.information_schema.columns";
        sqlHashagg = "select count(*),table_name from system.information_schema.tables group by table_name";
        sqlJoin = "select cl.table_catalog || cl.table_schema,cl.ordinal_position + 1,substr(cl.column_name,1,20) from system.information_schema.columns cl " +
                "left join system.information_schema.tables tb on cl.table_name = tb.table_name where cl.ordinal_position > 10 and is_nullable like 'Y%'";
        sqlWindow = "select min(ordinal_position) over (partition by column_name) minlong from system.information_schema.columns order by minlong";
        sqlFilter = "select * from system.information_schema.columns where ordinal_position between -1 and 100 or data_type='varchar' " +
                "and is_nullable is not null order by column_name";
        sqlRight = "select tb.table_catalog || tb.table_schema from system.information_schema.columns cl " +
                "right join system.information_schema.tables tb on cl.table_name = tb.table_name";
        sqlFull = "select tb.table_catalog || tb.table_schema from system.information_schema.columns cl " +
                "full join system.information_schema.tables tb on cl.table_name = tb.table_name";
    }
}
