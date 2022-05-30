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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.execution.ExplainAnalyzeContext;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.InternalBlockEncodingSerde;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.ExchangeOperator;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.HashAggregationOperator;
import io.prestosql.operator.HashBuilderOperator;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.LookupJoinOperatorFactory;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.ScanFilterAndProjectOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.operator.StreamingAggregationOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskOutputOperator;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.LocalDynamicFiltersCollector;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.SortExpressionContext;
import io.prestosql.sql.planner.SortExpressionExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import nova.hetu.olk.operator.AbstractOmniOperatorFactory;
import nova.hetu.olk.operator.AggregationOmniOperator;
import nova.hetu.olk.operator.BuildOffHeapOmniOperator;
import nova.hetu.olk.operator.BuildOnHeapOmniOperator;
import nova.hetu.olk.operator.DistinctLimitOmniOperator;
import nova.hetu.olk.operator.DynamicFilterSourceOmniOperator;
import nova.hetu.olk.operator.EnforceSingleRowOmniOperator;
import nova.hetu.olk.operator.HashAggregationOmniOperator;
import nova.hetu.olk.operator.HashBuilderOmniOperator.HashBuilderOmniOperatorFactory;
import nova.hetu.olk.operator.LimitOmniOperator;
import nova.hetu.olk.operator.LocalMergeSourceOmniOperator;
import nova.hetu.olk.operator.LookupJoinOmniOperators;
import nova.hetu.olk.operator.MergeOmniOperator;
import nova.hetu.olk.operator.PartitionedOutputOmniOperator;
import nova.hetu.olk.operator.ScanFilterAndProjectOmniOperator;
import nova.hetu.olk.operator.TopNOmniOperator;
import nova.hetu.olk.operator.WindowOmniOperator;
import nova.hetu.olk.operator.filterandproject.FilterAndProjectOmniOperator;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
import nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil;
import nova.hetu.olk.operator.localexchange.LocalExchangeSinkOmniOperator;
import nova.hetu.olk.operator.localexchange.LocalExchangeSourceOmniOperator;
import nova.hetu.olk.operator.localexchange.OmniLocalExchange;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.type.DataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverValueCount;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringWaitTime;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.prestosql.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.prestosql.SystemSessionProperties.getSpillOperatorThresholdReuseExchange;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.SystemSessionProperties.isSpillForOuterJoinEnabled;
import static io.prestosql.SystemSessionProperties.isSpillOrderBy;
import static io.prestosql.SystemSessionProperties.isSpillReuseExchange;
import static io.prestosql.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.spi.plan.JoinNode.Type.FULL;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.DynamicFilters.extractStaticFilters;
import static io.prestosql.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;
import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.expressionStringify;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;

/**
 * The type Omni local execution planner.
 *
 * @since 20210630
 */
public class OmniLocalExecutionPlanner
        extends LocalExecutionPlanner
{
    private static final Logger log = Logger.get(OmniLocalExecutionPlanner.class);

    private final OmniExpressionCompiler omniExpressionCompiler;

    /**
     * The Support types.
     */
    Set<String> supportTypes = new HashSet<String>()
    {
        {
            add(StandardTypes.INTEGER);
            add(StandardTypes.DATE);
            add(StandardTypes.BIGINT);
            add(StandardTypes.VARCHAR);
            add(StandardTypes.CHAR);
            add(StandardTypes.DECIMAL);
            add(StandardTypes.ROW);
            add(StandardTypes.DOUBLE);
            add(StandardTypes.VARBINARY);
            add(StandardTypes.BOOLEAN);
        }
    };

    public OmniLocalExecutionPlanner(LocalExecutionPlanner planner)
    {
        this(planner.getMetadata(), planner.getTypeAnalyzer(), planner.getExplainAnalyzeContext(),
                planner.getPageSourceProvider(), planner.getIndexManager(), planner.getNodePartitioningManager(),
                planner.getPageSinkManager(), planner.getExchangeClientSupplier(), planner.getExpressionCompiler(),
                planner.getPageFunctionCompiler(), planner.getJoinFilterFunctionCompiler(),
                planner.getIndexJoinLookupStats(), planner.getTaskManagerConfig(), planner.getSpillerFactory(),
                planner.getSingleStreamSpillerFactory(), planner.getPartitioningSpillerFactory(),
                planner.getPagesIndexFactory(), planner.getJoinCompiler(), planner.getLookupJoinOperators(),
                planner.getOrderingCompiler(), planner.getNodeInfo(), planner.getStateStoreProvider(),
                planner.getStateStoreListenerManager(), planner.getDynamicFilterCacheManager(),
                planner.getHeuristicIndexerManager(), planner.getCubeManager());
    }

    /**
     * Instantiates a new Omni local execution planner.
     *
     * @param metadata the metadata
     * @param typeAnalyzer the type analyzer
     * @param explainAnalyzeContext the explain analyze context
     * @param pageSourceProvider the page source provider
     * @param indexManager the index manager
     * @param nodePartitioningManager the node partitioning manager
     * @param pageSinkManager the page sink manager
     * @param exchangeClientSupplier the exchange client supplier
     * @param expressionCompiler the expression compiler
     * @param pageFunctionCompiler the page function compiler
     * @param joinFilterFunctionCompiler the join filter function compiler
     * @param indexJoinLookupStats the index join lookup stats
     * @param taskManagerConfig the task manager config
     * @param spillerFactory the spiller factory
     * @param singleStreamSpillerFactory the single stream spiller factory
     * @param partitioningSpillerFactory the partitioning spiller factory
     * @param pagesIndexFactory the pages index factory
     * @param joinCompiler the join compiler
     * @param lookupJoinOperators the lookup join operators
     * @param orderingCompiler the ordering compiler
     * @param nodeInfo the node info
     * @param stateStoreProvider the state store provider
     * @param stateStoreListenerManager the state store listener manager
     * @param dynamicFilterCacheManager the dynamic filter cache manager
     * @param heuristicIndexerManager the heuristic indexer manager
     */
    public OmniLocalExecutionPlanner(Metadata metadata, TypeAnalyzer typeAnalyzer,
                                     Optional<ExplainAnalyzeContext> explainAnalyzeContext, PageSourceProvider pageSourceProvider,
                                     IndexManager indexManager, NodePartitioningManager nodePartitioningManager, PageSinkManager pageSinkManager,
                                     ExchangeClientSupplier exchangeClientSupplier, ExpressionCompiler expressionCompiler,
                                     PageFunctionCompiler pageFunctionCompiler, JoinFilterFunctionCompiler joinFilterFunctionCompiler,
                                     IndexJoinLookupStats indexJoinLookupStats, TaskManagerConfig taskManagerConfig,
                                     SpillerFactory spillerFactory, SingleStreamSpillerFactory singleStreamSpillerFactory,
                                     PartitioningSpillerFactory partitioningSpillerFactory, PagesIndex.Factory pagesIndexFactory,
                                     JoinCompiler joinCompiler, LookupJoinOperators lookupJoinOperators, OrderingCompiler orderingCompiler,
                                     NodeInfo nodeInfo, StateStoreProvider stateStoreProvider,
                                     StateStoreListenerManager stateStoreListenerManager, DynamicFilterCacheManager dynamicFilterCacheManager,
                                     HeuristicIndexerManager heuristicIndexerManager, CubeManager cubeManager)
    {
        super(metadata, typeAnalyzer, explainAnalyzeContext, pageSourceProvider, indexManager, nodePartitioningManager,
                pageSinkManager, exchangeClientSupplier, expressionCompiler, pageFunctionCompiler,
                joinFilterFunctionCompiler, indexJoinLookupStats, taskManagerConfig, spillerFactory,
                singleStreamSpillerFactory, partitioningSpillerFactory, pagesIndexFactory, joinCompiler,
                lookupJoinOperators, orderingCompiler, nodeInfo, stateStoreProvider, stateStoreListenerManager,
                dynamicFilterCacheManager, heuristicIndexerManager, cubeManager);
        // use omni expressionCompiler
        this.omniExpressionCompiler = new OmniExpressionCompiler(metadata, pageFunctionCompiler);
    }

    static List<Integer> getAggregationChannels(Map<Symbol, AggregationNode.Aggregation> aggregations,
                                                Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : aggregations.keySet()) {
            AggregationNode.Aggregation aggregation = aggregations.get(symbol);
            List<RowExpression> arguments = aggregation.getArguments();
            if (arguments.size() != 0) {
                RowExpression rowExpression = arguments.get(0);
                checkState(rowExpression instanceof VariableReferenceExpression);
                Symbol aggregationInputSymbol = new Symbol(((VariableReferenceExpression) rowExpression).getName());
                builder.add(layout.get(aggregationInputSymbol));
            }
        }
        return builder.build();
    }

    static List<Type> getAggregationResultTypes(Map<Symbol, AggregationNode.Aggregation> aggregations,
                                                LocalExecutionPlanContext context)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (Symbol symbol : aggregations.keySet()) {
            builder.add(context.getTypes().get(symbol));
        }
        return builder.build();
    }

    static List<FunctionType> getAggregateTypes(List<Symbol> aggregationOutputSymbols,
                                                Map<Symbol, AggregationNode.Aggregation> aggregations)
    {
        ImmutableList.Builder<FunctionType> builder = ImmutableList.builder();
        for (Symbol aggregationOutputSymbol : aggregationOutputSymbols) {
            AggregationNode.Aggregation aggregation = aggregations.get(aggregationOutputSymbol);
            CallExpression functionCall = aggregation.getFunctionCall();
            List<RowExpression> arguments = aggregation.getArguments();
            // aggregator type, eg:sum,avg...
            switch (functionCall.getDisplayName()) {
                case "sum":
                    builder.add(OMNI_AGGREGATION_TYPE_SUM);
                    break;
                case "avg":
                    builder.add(OMNI_AGGREGATION_TYPE_AVG);
                    break;
                case "count": {
                    if (arguments.size() == 0) {
                        builder.add(OMNI_AGGREGATION_TYPE_COUNT_ALL);
                    }
                    else {
                        builder.add(OMNI_AGGREGATION_TYPE_COUNT_COLUMN);
                    }
                    break;
                }
                case "min":
                    builder.add(OMNI_AGGREGATION_TYPE_MIN);
                    break;
                case "max":
                    builder.add(OMNI_AGGREGATION_TYPE_MAX);
                    break;
                // TODO count *
                default:
                    throw new UnsupportedOperationException(
                            "unsupported Aggregator type by OmniRuntime: " + functionCall.getDisplayName());
            }
        }
        return builder.build();
    }

    @Override
    public LocalExecutionPlan plan(TaskContext taskContext, PlanNode plan, TypeProvider types,
                                   PartitioningScheme partitioningScheme, StageExecutionDescriptor stageExecutionDescriptor,
                                   List<PlanNodeId> partitionedSourceOrder, OutputBuffer outputBuffer, Optional<PlanFragmentId> feederCTEId,
                                   Optional<PlanNodeId> feederCTEParentId, Map<String, CommonTableExecutionContext> cteCtx)
    {
        VecAllocatorHelper.createTaskLevelAllocator(taskContext);
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)
                || partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)
                || partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION)
                || partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION)
                || partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder,
                    outputBuffer, new TaskOutputOperator.TaskOutputFactory(outputBuffer), feederCTEId,
                    feederCTEParentId, cteCtx);
        }

        // We can convert the symbols directly into channels, because the root must be a
        // sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return -1;
                }
                return outputLayout.indexOf(argument.getColumn());
            }).collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return Optional.of(argument.getConstant());
                }
                return Optional.<NullableValue>empty();
            }).collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream().map(argument -> {
                if (argument.isConstant()) {
                    return argument.getConstant().getType();
                }
                return types.get(argument.getColumn());
            }).collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(),
                partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero
        // columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }
        boolean isHashPrecomputed = partitioningScheme.getHashColumn().isPresent();
        return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder,
                outputBuffer,
                new PartitionedOutputOmniOperator.PartitionedOutputOmniFactory(partitionFunction, partitionChannels,
                        partitionConstants, partitioningScheme.isReplicateNullsAndAny(), nullChannel, outputBuffer,
                        maxPagePartitioningBufferSize, partitioningScheme.getBucketToPartition().get(),
                        isHashPrecomputed, partitionChannelTypes),
                feederCTEId, feederCTEParentId, cteCtx);
    }

    @Override
    public LocalExecutionPlan plan(TaskContext taskContext, StageExecutionDescriptor stageExecutionDescriptor,
                                   PlanNode plan, List<Symbol> outputLayout, TypeProvider types, List<PlanNodeId> partitionedSourceOrder,
                                   OutputBuffer outputBuffer, OutputFactory outputOperatorFactory, Optional<PlanFragmentId> feederCTEId,
                                   Optional<PlanNodeId> feederCTEParentId, Map<String, CommonTableExecutionContext> cteCtx)
    {
        VecAllocatorHelper.createTaskLevelAllocator(taskContext);
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new OmniLocalExecutionPlanContext(taskContext, types, metadata,
                dynamicFilterCacheManager, feederCTEId, feederCTEParentId, cteCtx);

        PhysicalOperation physicalOperation;
        try {
            physicalOperation = plan.accept(new OmniVisitor(session, stageExecutionDescriptor), context);
        }
        catch (Exception e) {
            log.warn("Unable to plan with OmniRuntime Operators for task: " + taskContext.getTaskId() + ", cause: "
                    + e.getLocalizedMessage());
            return null;
        }

        // if there is a data type not support by OmniRuntime, then fall back
        for (DriverFactory driverFactory : context.getDriverFactories()) {
            for (OperatorFactory operatorFactory : driverFactory.getOperatorFactories()) {
                if (notSupportTypes(operatorFactory.getSourceTypes())) {
                    log.warn("There is a data type not support by OmniRuntime: %s",
                            operatorFactory.getSourceTypes().toString());
                    return null;
                }
            }
        }
        for (OperatorFactory operatorFactory : physicalOperation.getOperatorFactories()) {
            if (notSupportTypes(operatorFactory.getSourceTypes())) {
                log.warn("There is a data type not support by OmniRuntime: %s",
                        operatorFactory.getSourceTypes().toString());
                return null;
            }
        }

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());
        List<Type> outputTypes = outputLayout.stream().map(types::get).collect(toImmutableList());

        context.addDriverFactory(context.isInputDriver(), true,
                ImmutableList.<OperatorFactory>builder().addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(context.getNextOperatorId(), plan.getId(),
                                outputTypes, pagePreprocessor, taskContext))
                        .build(),
                context.getDriverInstanceCount(), physicalOperation.getPipelineExecutionStrategy());
        addLookupOuterDrivers(context);
        addTransformOperators(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream().map(DriverFactory::getOperatorFactories).flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance).map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        log.debug("create the omni local execution plan successful!");
        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor,
                feederCTEId);
    }

    private static void addTransformOperators(LocalExecutionPlanContext context)
    {
        // Here we add a BuildOnHeapOmniOperator after OmniOperator when it's next
        // operator
        // is OlkOperator
        // or add a BuildOffHeapOmniOperator after OlkOperator when it's next operator
        // is
        // OmniOperator.
        // And their operator id number is odd.
        List<DriverFactory> newDriverFactories = new ArrayList<>();
        for (DriverFactory factory : context.getDriverFactories()) {
            List<OperatorFactory> operatorFactories = factory.getOperatorFactories();
            List<OperatorFactory> newOperatorFactories = new ArrayList<>();
            OperatorFactory currentOperatorFactory;
            OperatorFactory nextOperatorFactory;

            int index = 0;
            while (index < operatorFactories.size() - 1) {
                currentOperatorFactory = operatorFactories.get(index);
                nextOperatorFactory = operatorFactories.get(index + 1);

                boolean current = currentOperatorFactory.isExtensionOperatorFactory();
                boolean next = nextOperatorFactory.isExtensionOperatorFactory();

                newOperatorFactories.add(currentOperatorFactory);

                if (current && !next) {
                    // need to add a BuildOnHeapOmniOperator
                    BuildOnHeapOmniOperator.BuildOnHeapOmniOperatorFactory buildOnHeapOmniOperatorFactory = new BuildOnHeapOmniOperator.BuildOnHeapOmniOperatorFactory(
                            ((AbstractOmniOperatorFactory) currentOperatorFactory).getOperatorId() + 1, ((AbstractOmniOperatorFactory) currentOperatorFactory).getPlanNodeId());
                    newOperatorFactories.add(buildOnHeapOmniOperatorFactory);
                }
                if (!current && next) {
                    // need to add a BuildOffHeapOmniOperator
                    BuildOffHeapOmniOperator.BuildOffHeapOmniOperatorFactory buildOffHeapOmniOperatorFactory = new BuildOffHeapOmniOperator.BuildOffHeapOmniOperatorFactory(
                            ((AbstractOmniOperatorFactory) nextOperatorFactory).getOperatorId() - 1, ((AbstractOmniOperatorFactory) nextOperatorFactory).getPlanNodeId(), nextOperatorFactory.getSourceTypes());
                    newOperatorFactories.add(buildOffHeapOmniOperatorFactory);
                }
                index++;
            }
            newOperatorFactories.add(operatorFactories.get(index));

            DriverFactory driverFactory = new DriverFactory(factory.getPipelineId(), factory.isInputDriver(),
                    factory.isOutputDriver(), newOperatorFactories, factory.getDriverInstances(),
                    factory.getPipelineExecutionStrategy());
            newDriverFactories.add(driverFactory);
        }
        context.setDriverFactories(newDriverFactories);
    }

    private boolean notSupportTypes(List<Type> types)
    {
        for (Type type : types) {
            String base = type.getTypeSignature().getBase();
            if (!supportTypes.contains(base)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The type Omni visitor.
     *
     * @since 20210630
     */
    public class OmniVisitor
            extends Visitor
    {
        /**
         * Instantiates a new Omni visitor.
         *
         * @param session the session
         * @param stageExecutionDescriptor the stage execution descriptor
         */
        public OmniVisitor(Session session, StageExecutionDescriptor stageExecutionDescriptor)
        {
            super(session, stageExecutionDescriptor);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().getOrdering(symbol));
            }

            OperatorFactory operatorFactory = new TopNOmniOperator.TopNOmniOperatorFactory(context.getNextOperatorId(),
                    node.getId(), source.getTypes(), (int) node.getCount(), sortChannels, sortOrders);

            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOmniOperator.LimitOmniOperatorFactory(
                    context.getNextOperatorId(), node.getId(), node.getCount(), source.getTypes());
            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(
                    context.getNextOperatorId(), node.getId(), source.getTypes(), distinctChannels, hashChannel,
                    node.getLimit());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            RowExpression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression),
                    AssignmentUtils.identityAssignments(context.getTypes(), outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<RowExpression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(),
                    outputSymbols);
        }

        private PhysicalOperation visitScanFilterAndProject(LocalExecutionPlanContext context, PlanNodeId planNodeId,
                                                            PlanNode sourceNode, Optional<RowExpression> inputFilterExpression, Assignments assignments,
                                                            List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            ReuseExchangeOperator.STRATEGY strategy = REUSE_STRATEGY_DEFAULT;
            UUID reuseTableScanMappingId = new UUID(0, 0);
            Integer consumerTableScanNodeCount = 0;
            List<Type> inputTypes;
            Optional<RowExpression> filterExpression = inputFilterExpression;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                inputTypes = getSymbolTypes(tableScanNode.getOutputSymbols(), context.getTypes());

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }

                strategy = tableScanNode.getStrategy();
                reuseTableScanMappingId = tableScanNode.getReuseTableScanMappingId();
                consumerTableScanNodeCount = tableScanNode.getConsumerTableScanNodeCount();
            }
            // TODO: This is a simple hack, it will be replaced when we add ability to push
            // down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random
            // splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported",
                        sampleNode.getSampleType());
                return visitScanFilterAndProject(context, planNodeId, sampleNode.getSource(), inputFilterExpression,
                        assignments, outputSymbols);
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();

                inputTypes = source.getTypes();
            }
            boolean isLikeExpression = filterExpression.isPresent() && filterExpression.get() instanceof CallExpression
                    && "LIKE".equals(((CallExpression) filterExpression.get()).getDisplayName());
            Optional<RowExpression> rowExpression;
            if (isLikeExpression) {
                rowExpression = Optional.of(((CallExpression) filterExpression.get()).getArguments().get(1));
            }
            else {
                rowExpression = Optional.empty();
            }

            // filterExpression may contain large function calls; evaluate them before
            // compiling.
            if (filterExpression.isPresent()) {
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout, context.getTypes()));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            Optional<DynamicFilters.ExtractResult> extractDynamicFilterResult = filterExpression
                    .map(DynamicFilters::extractDynamicFilters);

            List<List<DynamicFilters.Descriptor>> extractDynamicFilterUnionResult;
            if (isCTEReuseEnabled(session)) {
                extractDynamicFilterUnionResult = DynamicFilters.extractDynamicFiltersAsUnion(filterExpression,
                        metadata);
            }
            else {
                if (extractDynamicFilterResult.isPresent()) {
                    extractDynamicFilterUnionResult = ImmutableList
                            .of(extractDynamicFilterResult.get().getDynamicConjuncts());
                }
                else {
                    extractDynamicFilterUnionResult = ImmutableList.of();
                }
            }

            Optional<RowExpression> translatedFilter = extractStaticFilters(filterExpression, metadata);

            // TODO: Execution must be plugged in here
            Supplier<List<Map<ColumnHandle, DynamicFilter>>> dynamicFilterSupplier = getDynamicFilterSupplier(
                    Optional.of(extractDynamicFilterUnionResult), sourceNode, context);
            Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
            if ((dynamicFilterSupplier != null && extractDynamicFilterResult.isPresent()
                    && !extractDynamicFilterResult.get().getDynamicConjuncts().isEmpty())
                    || (dynamicFilterSupplier != null && isCTEReuseEnabled(session)
                    && !extractDynamicFilterUnionResult.isEmpty())) {
                dynamicFilter = Optional.of(new DynamicFilterSupplier(dynamicFilterSupplier, System.currentTimeMillis(),
                        getDynamicFilteringWaitTime(session).toMillis()));
            }

            List<RowExpression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> bindChannels(expression, sourceLayout, context.getTypes()))
                    .collect(toImmutableList());

            boolean useOmniOperator = true;
            Supplier<PageProcessor> pageProcessor;
            Optional<RowExpression> omniTranslatedFilter = isLikeExpression
                    ? addRowExpression(rowExpression, translatedFilter)
                    : translatedFilter;
            pageProcessor = omniExpressionCompiler.compilePageProcessor(omniTranslatedFilter, translatedProjections,
                    Optional.of(context.getStageId() + "_" + planNodeId), OptionalInt.empty(), inputTypes,
                    context.getTaskId(), (OmniLocalExecutionPlanContext) context);

            if (pageProcessor == null) {
                useOmniOperator = false;
                pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections,
                        Optional.of(context.getStageId() + "_" + planNodeId));
            }

            if (columns != null) {
                boolean spillEnabled = isSpillEnabled(session) && isSpillReuseExchange(session);
                // convert from MB to bytes
                int spillerThreshold = getSpillOperatorThresholdReuseExchange(session) * 1024 * 1024;
                Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter,
                        translatedProjections, sourceNode.getId());
                SourceOperatorFactory operatorFactory;
                if (useOmniOperator) {
                    operatorFactory = new ScanFilterAndProjectOmniOperator.ScanFilterAndProjectOmniOperatorFactory(
                            context.getSession(), context.getNextOperatorId(), planNodeId, sourceNode,
                            pageSourceProvider, cursorProcessor, pageProcessor, table, columns, dynamicFilter,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            stateStoreProvider, metadata, dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session), strategy, reuseTableScanMappingId,
                            spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount,
                            inputTypes);
                }
                else {
                    operatorFactory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                            context.getSession(), context.getNextOperatorId(), planNodeId, sourceNode,
                            pageSourceProvider, cursorProcessor, pageProcessor, table, columns, dynamicFilter,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            stateStoreProvider, metadata, dynamicFilterCacheManager,
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session), strategy, reuseTableScanMappingId,
                            spillEnabled, Optional.of(spillerFactory), spillerThreshold, consumerTableScanNodeCount);
                }
                return new PhysicalOperation(operatorFactory, outputMappings, context,
                        stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId())
                                ? GROUPED_EXECUTION
                                : UNGROUPED_EXECUTION);
            }
            else {
                OperatorFactory operatorFactory;
                if (useOmniOperator) {
                    operatorFactory = new FilterAndProjectOmniOperator.FilterAndProjectOmniOperatorFactory(
                            context.getNextOperatorId(), planNodeId, pageProcessor,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session), inputTypes);
                }
                else {
                    operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(), planNodeId, pageProcessor,
                            projections.stream().map(expression -> expression.getType()).collect(toImmutableList()),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));
                }
                return new PhysicalOperation(operatorFactory, outputMappings, context, source);
            }
        }

        private Optional<RowExpression> addRowExpression(Optional<RowExpression> rowExpression,
                                                         Optional<RowExpression> translatedFilter)
        {
            List<RowExpression> newArgs = new LinkedList<RowExpression>();
            newArgs.add(((CallExpression) translatedFilter.get()).getArguments().get(0));
            newArgs.add(rowExpression.get());
            Optional<RowExpression> likeTranslatedFilter = Optional
                    .of(new CallExpression(((CallExpression) translatedFilter.get()).getDisplayName(),
                            ((CallExpression) translatedFilter.get()).getFunctionHandle(),
                            ((CallExpression) translatedFilter.get()).getType(), newArgs));
            return likeTranslatedFilter;
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOmniOperator.EnforceSingleRowOmniOperatorFactory(
                    context.getNextOperatorId(), node.getId(), source.getTypes());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());

            // Snapshot: current implementation depends on the fact that local-merge only
            // uses pass-through exchangers
            checkArgument(
                    node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_PASSTHROUGH_DISTRIBUTION));

            LocalExchange.LocalExchangeFactory exchangeFactory = new OmniLocalExchange.OmniLocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(), operatorsCount, types,
                    ImmutableList.of(), Optional.empty(), source.getPipelineExecutionStrategy(),
                    maxLocalExchangeBufferSize, true, node.getAggregationType());

            List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());
            List<Symbol> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());

            operatorFactories.add(new LocalExchangeSinkOmniOperator.LocalExchangeSinkOmniOperatorFactory(
                    exchangeFactory, subContext.getNextOperatorId(), node.getId(), exchangeFactory.newSinkFactoryId(),
                    pagePreprocessor, types));

            context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories,
                    subContext.getDriverInstanceCount(), source.getPipelineExecutionStrategy());
            // the main driver is not an input... the exchange sources are the input for the
            // plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            OperatorFactory operatorFactory = new LocalMergeSourceOmniOperator.LocalMergeSourceOmniOperatorFactory(
                    context.getNextOperatorId(), context.getNextOperatorId(), node.getId(), exchangeFactory, types,
                    orderingCompiler, sortChannels, orderings, outputChannels.build());

            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        public PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session);
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            List<Integer> channels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn())).collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol));

            PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = GROUPED_EXECUTION;
            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));

                if (source.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    exchangeSourcePipelineExecutionStrategy = UNGROUPED_EXECUTION;
                }
            }

            // Because OmniLocalExchange extends from LocalExchange and replaces
            // PartitioningExchanger with OmniPartitioningExchanger, where the omni block
            // will be closed inside, we use OmniLocalExchangeFactory to generate
            // LocalExchangeSinkOmniOperator and LocalExchangeSourceOmniOperatorFactory.
            LocalExchange.LocalExchangeFactory localExchangeFactory = new OmniLocalExchange.OmniLocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(), driverInstanceCount, types, channels,
                    hashChannel, exchangeSourcePipelineExecutionStrategy, maxLocalExchangeBufferSize, false,
                    node.getAggregationType());

            int totalSinkCount = 0;
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                operatorFactories.add(new LocalExchangeSinkOmniOperator.LocalExchangeSinkOmniOperatorFactory(
                        localExchangeFactory, subContext.getNextOperatorId(), node.getId(),
                        localExchangeFactory.newSinkFactoryId(), pagePreprocessor, types));

                context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories,
                        subContext.getDriverInstanceCount(), exchangeSourcePipelineExecutionStrategy);

                // Snapshot: total number of sinks are the expected number of input channels for
                // local-exchange source
                totalSinkCount += subContext.getDriverInstanceCount().orElse(1);
                // For each outer-join, also need to include the lookup-outer driver as a sink
                totalSinkCount = (int) (totalSinkCount + operatorFactories.stream()
                        .filter(factory -> factory instanceof LookupJoinOperatorFactory
                                && ((LookupJoinOperatorFactory) factory).createOuterOperatorFactory().isPresent())
                        .count());
            }

            // the main driver is not an input... the exchange sources are the input for the
            // plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchangeFactory.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            OperatorFactory operatorFactory = new LocalExchangeSourceOmniOperator.LocalExchangeSourceOmniOperatorFactory(
                    context.getNextOperatorId(), node.getId(), localExchangeFactory, totalSinkCount, types);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context,
                    exchangeSourcePipelineExecutionStrategy);
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }
            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size()).boxed().collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOmniOperator.MergeOmniOperatorFactory(
                    context.getNextOperatorId(), context.getNextOperatorId(), node.getId(), exchangeClientSupplier,
                    new PagesSerdeFactory(new InternalBlockEncodingSerde(metadata.getFunctionAndTypeManager()),
                            isExchangeCompressionEnabled(session)),
                    orderingCompiler, types, outputChannels, sortChannels, sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (!context.getDriverInstanceCount().isPresent()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            log.debug("using OmniInternalBlockEncodingSerde!");
            OperatorFactory operatorFactory = new ExchangeOperator.ExchangeOperatorFactory(context.getNextOperatorId(),
                    node.getId(), exchangeClientSupplier);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession());
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(context.getSession());

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source,
                                                        LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createAggregationOperatorFactory(node.getId(), node.getAggregations(),
                    node.getStep(), 0, outputMappings, source, context, node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private OperatorFactory createAggregationOperatorFactory(PlanNodeId planNodeId,
                                                                 Map<Symbol, AggregationNode.Aggregation> aggregations, AggregationNode.Step step,
                                                                 int startOutputChannel, ImmutableMap.Builder<Symbol, Integer> outputMappings, PhysicalOperation source,
                                                                 LocalExecutionPlanContext context, boolean useSystemMemory)
        {
            int outputChannel = startOutputChannel;
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            ImmutableList.Builder<AccumulatorFactory> accumulatorFactories = ImmutableList.builder();
            ImmutableList.Builder<Optional<Integer>> maskChannels = ImmutableList.builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                AggregationNode.Aggregation aggregation = entry.getValue();
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                maskChannels.add(aggregation.getMask().map(value -> source.getLayout().get(value)));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                aggregationOutputSymbols.add(symbol);
                outputChannel++;
            }
            ImmutableList<AccumulatorFactory> factories = accumulatorFactories.build();

            // use omni aggregation operator
            List<Integer> aggregationChannels = getAggregationChannels(aggregations, source.getLayout());
            List<Type> aggregationResultTypes = getAggregationResultTypes(aggregations, context);
            FunctionType[] aggregatorTypes = getAggregateTypes(aggregationOutputSymbols, aggregations)
                    .toArray(new FunctionType[aggregations.size()]);
            int[] aggregationInputChannels = Ints.toArray(aggregationChannels);
            DataType[] aggregationOutputTypes = OperatorUtils.toDataTypes(aggregationResultTypes);

            return new AggregationOmniOperator.AggregationOmniOperatorFactory(context.getNextOperatorId(), planNodeId,
                    source.getTypes(), aggregatorTypes, aggregationInputChannels, maskChannels.build(),
                    aggregationOutputTypes, step);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source,
                                                         boolean spillEnabled, DataSize unspillMemoryLimit, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(node.getId(), node.getAggregations(),
                    node.getGlobalGroupingSets(), node.getGroupingKeys(), node.getStep(), node.getHashSymbol(),
                    node.getGroupIdSymbol(), source, node.hasDefaultOutput(), spillEnabled, node.isStreamable(),
                    unspillMemoryLimit, context, 0, mappings, 10_000, Optional.of(maxPartialAggregationMemorySize),
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, mappings.build(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(PlanNodeId planNodeId,
                                                                     Map<Symbol, AggregationNode.Aggregation> aggregations, Set<Integer> globalGroupingSets,
                                                                     List<Symbol> groupBySymbols, AggregationNode.Step step, Optional<Symbol> hashSymbol,
                                                                     Optional<Symbol> groupIdSymbol, PhysicalOperation source, boolean hasDefaultOutput,
                                                                     boolean spillEnabled, boolean isStreamable, DataSize unspillMemoryLimit,
                                                                     LocalExecutionPlanContext context, int startOutputChannel,
                                                                     ImmutableMap.Builder<Symbol, Integer> outputMappings, int expectedGroups,
                                                                     Optional<DataSize> maxPartialAggregationMemorySize, boolean useSystemMemory)
        {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            ImmutableList.Builder<Optional<Integer>> maskChannels = ImmutableList.builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                AggregationNode.Aggregation aggregation = entry.getValue();
                maskChannels.add(aggregation.getMask().map(value -> source.getLayout().get(value)));

                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashSymbol.isPresent()) {
                outputMappings.put(hashSymbol.get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Integer> aggregationChannels = getAggregationChannels(aggregations, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream().map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());
            List<Type> aggregationSourceTypes = aggregationChannels.stream().map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());
            List<Type> aggregationResultTypes = getAggregationResultTypes(aggregations, context);

            if (isStreamable) {
                return new StreamingAggregationOperator.StreamingAggregationOperatorFactory(context.getNextOperatorId(),
                        planNodeId, source.getTypes(), groupByTypes, groupByChannels, step, accumulatorFactories,
                        joinCompiler);
            }
            else {
                // right now HashAggregationOmniOperator does not support the groupIdChannel, we
                // should fall back
                if (!groupIdChannel.isPresent()) {
                    // when omni is turned on there is no hash channel
                    int[] groupByInputChannels = Ints.toArray(groupByChannels);
                    DataType[] groupByInputTypes = OperatorUtils.toDataTypes(groupByTypes);
                    int[] aggregationInputChannels = Ints.toArray(aggregationChannels);
                    DataType[] aggregationInputTypes = OperatorUtils.toDataTypes(aggregationSourceTypes);
                    DataType[] aggregationOutputTypes = OperatorUtils.toDataTypes(aggregationResultTypes);
                    FunctionType[] aggregatorTypes = getAggregateTypes(aggregationOutputSymbols, aggregations)
                            .toArray(new FunctionType[aggregations.size()]);

                    return new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(
                            context.getNextOperatorId(), planNodeId, source.getTypes(), groupByInputChannels,
                            groupByInputTypes, aggregationInputChannels, aggregationInputTypes, aggregatorTypes,
                            maskChannels.build(), aggregationOutputTypes, step);
                }
                else {
                    Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
                    return new HashAggregationOperator.HashAggregationOperatorFactory(context.getNextOperatorId(),
                            planNodeId, groupByTypes, groupByChannels, ImmutableList.copyOf(globalGroupingSets), step,
                            hasDefaultOutput, accumulatorFactories, hashChannel, groupIdChannel, expectedGroups,
                            maxPartialAggregationMemorySize, spillEnabled, unspillMemoryLimit, spillerFactory,
                            joinCompiler, useSystemMemory);
                }
            }
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().getOrdering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession()) && isSpillOrderBy(context.getSession());

            OperatorFactory operator = createOrderByOmniOperatorFactory(context.getNextOperatorId(), node.getId(),
                    source.getTypes(), outputChannels.build(), orderByChannels, sortOrder.build());

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList
                    .copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(
                    getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }
            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();

                WindowNode.Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }

                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }

                FrameInfo frameInfo = new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel,
                        frame.getEndType(), frameEndChannel);

                FunctionHandle functionHandle = entry.getValue().getFunctionHandle();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (RowExpression argument : entry.getValue().getArguments()) {
                    checkState(argument instanceof VariableReferenceExpression);
                    Symbol argumentSymbol = new Symbol(((VariableReferenceExpression) argument).getName());
                    arguments.add(source.getLayout().get(argumentSymbol));
                }

                Symbol symbol = entry.getKey();
                WindowFunctionSupplier windowFunctionSupplier = metadata.getFunctionAndTypeManager()
                        .getWindowFunctionImplementation(functionHandle);
                Type type = metadata.getType(entry.getValue().getFunctionCall().getType().getTypeSignature());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel
            // from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            OperatorFactory operatorFactory = new WindowOmniOperator.WindowOmniOperatorFactory(
                    context.getNextOperatorId(), node.getId(), source.getTypes(), outputChannels.build(),
                    windowFunctionsBuilder.build(), partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                    node.getPreSortedOrderPrefix(), 10_000);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // TODO: Execution must be plugged in here
            if (!node.getDynamicFilters().isEmpty()) {
                // log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            }

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(),
                            node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        public JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(JoinNode node,
                                                                                           PlanNode buildNode, List<Symbol> buildSymbols, Optional<Symbol> buildHashSymbol,
                                                                                           PhysicalOperation probeSource, LocalExecutionPlanContext context, boolean spillEnabled)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol)).collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList
                    .copyOf(getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList
                    .copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource)).map(OptionalInt::of)
                    .orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int taskCount = buildContext.getDriverInstanceCount().orElse(1);
            boolean canOuterSpill = isSpillForOuterJoinEnabled(session);
            boolean spillAllowed = spillEnabled;
            if (buildOuter && spillEnabled) {
                spillAllowed = canOuterSpill;
            }
            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream().map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter, probeSource.getPipelineExecutionStrategy(), buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(buildSource.getTypes(), buildOutputTypes,
                            buildChannels.stream().map(buildSource.getTypes()::get).collect(toImmutableList()),
                            taskCount, buildSource.getLayout(), buildOuter, canOuterSpill),
                    buildOutputTypes);

            ImmutableList.Builder<OperatorFactory> factoriesBuilder = new ImmutableList.Builder();
            factoriesBuilder.addAll(buildSource.getOperatorFactories());

            createDynamicFilter(node, context, taskCount).ifPresent(filter -> {
                List<DynamicFilterSourceOmniOperator.Channel> filterBuildChannels = filter.getBuildChannels().entrySet()
                        .stream().map(entry -> {
                            String filterId = entry.getKey();
                            int index = entry.getValue();
                            Type type = buildSource.getTypes().get(index);
                            return new DynamicFilterSourceOmniOperator.Channel(filterId, type, index,
                                    context.getSession().getQueryId().toString());
                        }).collect(Collectors.toList());
                factoriesBuilder.add(new DynamicFilterSourceOmniOperator.DynamicFilterSourceOmniOperatorFactory(
                        buildContext.getNextOperatorId(), node.getId(), filter.getValueConsumer(),
                        /** the consumer to process all values collected to build the dynamic filter */
                        filterBuildChannels, getDynamicFilteringMaxPerDriverValueCount(buildContext.getSession()),
                        getDynamicFilteringMaxPerDriverSize(buildContext.getSession()), buildSource.getTypes()));
            });

            // if it is right join or full join, we do not use omni operators
            if (!buildOuter) {
                Optional<String> filterFunction = node.getFilter()
                        .map(filterExpression -> getTranslatedExpression(context, buildSource, probeSource,
                                filterExpression));
                Optional<SortExpressionContext> sortExpressionContext = node.getFilter()
                        .flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata,
                                node.getRightOutputSymbols(), filter));

                Optional<Integer> sortChannel = sortExpressionContext.map(SortExpressionContext::getSortExpression)
                        .map(sortExpression -> sortExpressionAsSortChannel(sortExpression, probeSource.getLayout(),
                                buildSource.getLayout(), context));

                List<String> searchFunctions = sortExpressionContext
                        .map(SortExpressionContext::getSearchExpressions).map(
                                searchExpressions -> searchExpressions.stream()
                                        .map(searchExpression -> getTranslatedExpression(context, buildSource,
                                                probeSource, searchExpression))
                                        .collect(toImmutableList()))
                        .orElse(ImmutableList.of());

                HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory = new HashBuilderOmniOperatorFactory(
                        buildContext.getNextOperatorId(), node.getId(), lookupSourceFactoryManager,
                        buildSource.getTypes(), buildOutputChannels, buildChannels, buildHashChannel, filterFunction,
                        sortChannel, searchFunctions, taskCount);
                factoriesBuilder.add(hashBuilderOmniOperatorFactory);
            }
            else {
                Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                        .map(filterExpression -> compileJoinFilterFunction(filterExpression, probeSource.getLayout(),
                                buildSource.getLayout(), context.getTypes(), context.getSession()));

                Optional<SortExpressionContext> sortExpressionContext = node.getFilter()
                        .flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata,
                                node.getRightOutputSymbols(), filter));

                Optional<Integer> sortChannel = sortExpressionContext.map(SortExpressionContext::getSortExpression)
                        .map(sortExpression -> sortExpressionAsSortChannel(sortExpression, probeSource.getLayout(),
                                buildSource.getLayout(), context));

                List<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                        .map(SortExpressionContext::getSearchExpressions)
                        .map(searchExpressions -> searchExpressions.stream()
                                .map(searchExpression -> compileJoinFilterFunction(searchExpression,
                                        probeSource.getLayout(), buildSource.getLayout(), context.getTypes(),
                                        context.getSession()))
                                .collect(toImmutableList()))
                        .orElse(ImmutableList.of());

                HashBuilderOperator.HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperator.HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, buildOutputChannels,
                        buildChannels, buildHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories,
                        10_000, pagesIndexFactory, spillAllowed && taskCount > 1, singleStreamSpillerFactory);

                factoriesBuilder.add(hashBuilderOperatorFactory);
            }

            context.addDriverFactory(buildContext.isInputDriver(), false, factoriesBuilder.build(),
                    buildContext.getDriverInstanceCount(), buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }

        private String getTranslatedExpression(LocalExecutionPlanContext context, PhysicalOperation buildSource,
                                               PhysicalOperation probeSource, RowExpression expression)
        {
            // In omni runtime, the join layout is probeLayout + buildLayout
            ImmutableMap.Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            Map<Symbol, Integer> probeSourceLayout = probeSource.getLayout();
            Map<Symbol, Integer> buildSourceLayout = buildSource.getLayout();
            joinSourcesLayout.putAll(probeSourceLayout);
            for (Map.Entry<Symbol, Integer> buildLayoutEntry : buildSourceLayout.entrySet()) {
                joinSourcesLayout.put(buildLayoutEntry.getKey(),
                        buildLayoutEntry.getValue() + probeSourceLayout.size());
            }

            RowExpression translatedExpression = bindChannels(expression, joinSourcesLayout.build(),
                    context.getTypes());
            return expressionStringify(translatedExpression, OmniRowExpressionUtil.Format.JSON);
        }

        public PhysicalOperation createLookupJoin(JoinNode node, PlanNode probeNode, List<Symbol> probeSymbols,
                                                  Optional<Symbol> probeHashSymbol, PlanNode buildNode, List<Symbol> buildSymbols,
                                                  Optional<Symbol> buildHashSymbol, LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean spillEnabled = isSpillEnabled(session)
                    && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"));
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = createLookupSourceFactory(node,
                    buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled);

            OperatorFactory operator = createLookupJoin(node, probeSource, probeSymbols, probeHashSymbol,
                    lookupSourceFactory, context, spillEnabled);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        public OperatorFactory createLookupJoin(JoinNode node, PhysicalOperation probeSource, List<Symbol> probeSymbols,
                                                Optional<Symbol> probeHashSymbol, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                                                LocalExecutionPlanContext context, boolean spillEnabled)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol)).collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList
                    .copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList
                    .copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource)).map(OptionalInt::of)
                    .orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            checkState(!spillEnabled || totalOperatorsCount.isPresent(),
                    "A fixed distribution is required for JOIN when spilling is enabled");

            // if it is right join or full join, we do not use omni operators
            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            if (!buildOuter) {
                return createOmniLookupJoin(node, lookupSourceFactoryManager, context, probeTypes, probeOutputChannels,
                        probeJoinChannels, probeHashChannel, totalOperatorsCount);
            }
            return getLookUpJoinOperatorFactory(node, lookupSourceFactoryManager, context, probeTypes,
                    probeOutputChannels, probeJoinChannels, probeHashChannel, totalOperatorsCount);
        }

        /**
         * Create omni lookup join operator factory.
         *
         * @param node the node
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param context the context
         * @param probeTypes the probe types
         * @param probeOutputChannels the probe output channels
         * @param probeJoinChannels the probe join channels
         * @param probeHashChannel the probe hash channel
         * @param totalOperatorsCount the total operators count
         * @return the operator factory
         */
        public OperatorFactory createOmniLookupJoin(JoinNode node,
                                                    JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                                                    LocalExecutionPlanContext context, List<Type> probeTypes, List<Integer> probeOutputChannels,
                                                    List<Integer> probeJoinChannels, OptionalInt probeHashChannel, OptionalInt totalOperatorsCount)
        {
            List<DriverFactory> driverFactories = context.getDriverFactories();
            DriverFactory driverFactory = driverFactories.get(driverFactories.size() - 1);
            List<OperatorFactory> operatorFactories = driverFactory.getOperatorFactories();
            OperatorFactory buildOperatorFactory = operatorFactories.get(operatorFactories.size() - 1);

            switch (node.getType()) {
                case INNER:
                    return LookupJoinOmniOperators.innerJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount,
                            (HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case LEFT:
                    return LookupJoinOmniOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount,
                            (HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case RIGHT:
                    return LookupJoinOmniOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount,
                            (HashBuilderOmniOperatorFactory) buildOperatorFactory);
                case FULL:
                    return LookupJoinOmniOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount,
                            (HashBuilderOmniOperatorFactory) buildOperatorFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type by OmniRuntime: " + node.getType());
            }
        }

        private OperatorFactory getLookUpJoinOperatorFactory(JoinNode node,
                                                             JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                                                             LocalExecutionPlanContext context, List<Type> probeTypes, List<Integer> probeOutputChannels,
                                                             List<Integer> probeJoinChannels, OptionalInt probeHashChannel, OptionalInt totalOperatorsCount)
        {
            switch (node.getType()) {
                case INNER:
                    return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);

                case LEFT:
                    return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);

                case RIGHT:
                    return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);

                case FULL:
                    return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(),
                            lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel,
                            Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        public RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types,
                                             Map<Symbol, Integer> layout)
        {
            return SqlToRowExpressionTranslator.translate(expression, SCALAR, types, layout,
                    metadata.getFunctionAndTypeManager(), session, true);
        }
    }

    /**
     * LocalExecutionPlanContext for OmniRuntime
     */
    public static class OmniLocalExecutionPlanContext
            extends LocalExecutionPlanContext
    {
        public OmniLocalExecutionPlanContext(TaskContext taskContext, TypeProvider types, Metadata metadata,
                                             DynamicFilterCacheManager dynamicFilterCacheManager, Optional<PlanFragmentId> feederCTEId,
                                             Optional<PlanNodeId> feederCTEParentId, Map<String, CommonTableExecutionContext> cteCtx)
        {
            super(taskContext, types, metadata, dynamicFilterCacheManager, feederCTEId, feederCTEParentId, cteCtx);
        }

        private OmniLocalExecutionPlanContext(TaskContext taskContext, TypeProvider types,
                                              List<DriverFactory> driverFactories, Optional<IndexSourceContext> indexSourceContext,
                                              LocalDynamicFiltersCollector dynamicFiltersCollector, AtomicInteger nextPipelineId,
                                              Optional<PlanFragmentId> feederCTEId, Optional<PlanNodeId> feederCTEParentId,
                                              Map<String, CommonTableExecutionContext> cteCtx)
        {
            super(taskContext, types, driverFactories, indexSourceContext, dynamicFiltersCollector, nextPipelineId,
                    feederCTEId, feederCTEParentId, cteCtx);
        }

        @Override
        public LocalExecutionPlanContext createSubContext()
        {
            checkState(!getIndexSourceContext().isPresent(), "index build plan can not have sub-contexts");
            return new OmniLocalExecutionPlanContext(taskContext, types, driverFactories, indexSourceContext,
                    dynamicFiltersCollector, nextPipelineId, feederCTEId, feederCTEParentId, cteCtx);
        }

        /**
         * Register callback when task finishes
         *
         * @param taskFinishHandler callback function when task finishes
         */
        public void onTaskFinished(Consumer<Boolean> taskFinishHandler)
        {
            taskContext.onTaskFinished(taskFinishHandler);
        }

        /**
         * Get next operator id twice and only use thr former
         */
        @Override
        public int getNextOperatorId()
        {
            int id = super.getNextOperatorId();
            super.getNextOperatorId();
            return id;
        }
    }
}
