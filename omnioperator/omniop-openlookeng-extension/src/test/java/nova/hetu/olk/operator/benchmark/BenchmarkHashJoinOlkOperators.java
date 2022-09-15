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

package nova.hetu.olk.operator.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.LookupSourceProvider;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.type.TypeUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Threads(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkHashJoinOlkOperators
{
    private static final int HASH_BUILD_OPERATOR_ID = 1;
    private static final int HASH_JOIN_OPERATOR_ID = 2;
    private static final PlanNodeId TEST_PLAN_NODE_ID = new PlanNodeId("test");
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();

    @State(Thread)
    public static class BuildContext
            extends AbstractOperatorBenchmarkContext
    {
        protected static final int ROWS_PER_PAGE = 10240;
        protected static final int BUILD_ROWS_NUMBER = 8_000_000;
        protected static final String PREFIX = "";

        protected static final Map<String, ImmutableList<Type>> BUILD_TYPES = ImmutableMap
                .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(BIGINT, createVarcharType(20)))
                .put("group2",
                        ImmutableList.of(BIGINT, INTEGER, createVarcharType(50), INTEGER, INTEGER,
                                createVarcharType(50), createVarcharType(10)))
                .put("group3", ImmutableList.of(BIGINT, createVarcharType(10)))
                .put("group4", ImmutableList.of(BIGINT, createVarcharType(50), createVarcharType(50)))
                .put("group5",
                        ImmutableList.of(BIGINT, INTEGER, createVarcharType(50), INTEGER, INTEGER,
                                createVarcharType(50), createVarcharType(10)))
                .put("group6", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, INTEGER))
                .put("group7", ImmutableList.of(BIGINT)).put("group8", ImmutableList.of(BIGINT, INTEGER, INTEGER))
                .put("group9", ImmutableList.of(BIGINT)).put("group10", ImmutableList.of(BIGINT))
                .put("group11", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, INTEGER))
                .put("group12",
                        ImmutableList.of(createVarcharType(50), createVarcharType(50), BIGINT, createVarcharType(50),
                                createVarcharType(50), BIGINT))
                .put("group13", ImmutableList.of(createVarcharType(50), INTEGER, BIGINT))
                .put("group14", ImmutableList.of(createDecimalType(12, 2), BIGINT, createVarcharType(50), INTEGER))
                .build();

        protected static final Map<String, ImmutableList<Integer>> BUILD_OUTPUT_COLS = ImmutableMap
                .<String, ImmutableList<Integer>>builder().put("group1", ImmutableList.of(1))
                .put("group2", ImmutableList.of(1, 2, 3, 4, 5)).put("group3", ImmutableList.of(1))
                .put("group4", ImmutableList.of(1, 2)).put("group5", ImmutableList.of(1, 2, 3, 4, 5, 6))
                .put("group6", ImmutableList.of(0, 1, 3, 4, 5)).put("group7", ImmutableList.of())
                .put("group8", ImmutableList.of(1, 2)).put("group9", ImmutableList.of())
                .put("group10", ImmutableList.of()).put("group11", ImmutableList.of(0, 2, 3, 4, 5, 6))
                .put("group12", ImmutableList.of(2)).put("group13", ImmutableList.of(0, 1))
                .put("group14", ImmutableList.of(0, 2, 3))
                .build();

        protected static final Map<String, ImmutableList<Integer>> BUILD_HASH_COLS = ImmutableMap
                .<String, ImmutableList<Integer>>builder().put("group1", ImmutableList.of(0))
                .put("group2", ImmutableList.of(0)).put("group3", ImmutableList.of(0))
                .put("group4", ImmutableList.of(0)).put("group5", ImmutableList.of(0))
                .put("group6", ImmutableList.of(2)).put("group7", ImmutableList.of(0))
                .put("group8", ImmutableList.of(0)).put("group9", ImmutableList.of(0))
                .put("group10", ImmutableList.of(0)).put("group11", ImmutableList.of(1))
                .put("group12", ImmutableList.of(3, 0, 4, 1, 5)).put("group13", ImmutableList.of(2))
                .put("group14", ImmutableList.of(1)).build();

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7", "group8", "group9", "group10",
                "group11", "group12", "group13", "group14"})
        protected String testGroup;

        @Param({"false", "true"})
        protected boolean isDictionaryBlocks;

        @Param({"false", "true"})
        protected boolean buildHashEnabled;

        @Param({"1", "5"})
        protected int buildRowsRepetition = 1;

        protected List<Page> buildPages = new ArrayList<>();
        protected List<Type> buildTypes;
        protected List<Integer> buildOutputChannels;
        protected List<Integer> buildJoinChannels;
        protected OptionalInt buildHashChannel;

        protected JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;

        @Override
        protected void beforeSetupTrial()
        {
            buildTypes = BUILD_TYPES.get(testGroup);
            buildOutputChannels = BUILD_OUTPUT_COLS.get(testGroup);
            buildJoinChannels = BUILD_HASH_COLS.get(testGroup);
            buildHashChannel = OptionalInt.empty();
            initializeBuildPages();
        }

        @Override
        protected void beforeSetupIteration()
        {
            operatorFactory = createOperatorFactory();
        }

        @Override
        protected List<Page> buildPages()
        {
            return buildPages;
        }

        @Override
        protected List<Page> forkPages(List<Page> pages)
        {
            return forkBuildPage(pages);
        }

        protected List<Page> forkBuildPage(List<Page> pages)
        {
            List<Page> slicedPages = new ArrayList<>(pages.size());
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            HashBuilderOperatorFactory hashBuilderOperatorFactory = createBuildOperatorFactory();
            LOOKUP_JOIN_OPERATORS.innerJoin(HASH_JOIN_OPERATOR_ID, TEST_PLAN_NODE_ID, lookupSourceFactoryManager,
                    getBuildTypes(), buildJoinChannels, buildHashChannel,
                    Optional.of(buildOutputChannels), OptionalInt.empty(),
                    unsupportedPartitioningSpillerFactory());
            return hashBuilderOperatorFactory;
        }

        @Override
        protected void beforeCleanupIteration()
        {
            finishBuildHash(lookupSourceFactoryManager);
        }

        protected HashBuilderOperatorFactory createBuildOperatorFactory()
        {
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryJoinBridgeManager = JoinBridgeManager
                    .lookupAllAtOnce(new PartitionedLookupSourceFactory(getBuildTypes(),
                            buildOutputChannels.stream().map(getBuildTypes()::get)
                                    .collect(toImmutableList()),
                            buildJoinChannels.stream().map(getBuildTypes()::get)
                                    .collect(toImmutableList()),
                            1, requireNonNull(ImmutableMap.of(), "layout is null"), false, false));
            this.lookupSourceFactoryManager = lookupSourceFactoryJoinBridgeManager;
            return new HashBuilderOperatorFactory(HASH_BUILD_OPERATOR_ID,
                    TEST_PLAN_NODE_ID, lookupSourceFactoryJoinBridgeManager, buildOutputChannels,
                    buildJoinChannels, buildHashChannel, Optional.empty(), Optional.empty(),
                    ImmutableList.of(), 10_000, new PagesIndex.TestingFactory(false), false,
                    SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());
        }

        public void buildHash(Operator operator, List<Page> inputPages)
        {
            Iterator<Page> iterator = inputPages.iterator();
            while (iterator.hasNext()) {
                Page next = iterator.next();
                operator.addInput(next);
                iterator.remove();
            }
            operator.finish();
        }

        public void finishBuildHash(JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager)
        {
            LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());
            ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
            if (!lookupSourceProvider.isDone()) {
                throw new AssertionError("Expected lookup source provider to be ready");
            }
            getFutureValue(lookupSourceProvider).close();
        }

        @Override
        protected TaskContext createTaskContext()
        {
            return createTaskContextBySizeInGigaByte(4);
        }

        protected List<Type> getBuildTypes()
        {
            if (buildHashEnabled) {
                return ImmutableList.copyOf(Iterables.concat(buildTypes, ImmutableList.of(BIGINT)));
            }
            return buildTypes;
        }

        protected void initializeBuildPages()
        {
            List<List<Integer>> columnValues = new ArrayList<>();
            for (int i = 0; i < buildTypes.size(); i++) {
                List<Integer> values = new ArrayList<>();
                columnValues.add(values);
            }

            int maxValue = BUILD_ROWS_NUMBER / buildRowsRepetition + 40;
            int rows = 0;
            while (rows < BUILD_ROWS_NUMBER) {
                int newRows = Math.min(BUILD_ROWS_NUMBER - rows, ROWS_PER_PAGE);
                for (int i = 0; i < buildTypes.size(); i++) {
                    Type type = buildTypes.get(i);
                    List<Integer> values = columnValues.get(i);
                    int initialValue;
                    if (type instanceof VarcharType) {
                        initialValue = (rows + 20) % maxValue;
                        for (int j = 0; j < newRows; j++) {
                            values.add(initialValue + j);
                        }
                    }
                    else if (type instanceof BigintType) {
                        initialValue = (rows + 30) % maxValue;
                        for (int j = 0; j < newRows; j++) {
                            values.add(initialValue + j);
                        }
                    }
                    else if (type instanceof IntegerType) {
                        initialValue = (rows + 40) % maxValue;
                        for (int j = 0; j < newRows; j++) {
                            values.add(initialValue + j);
                        }
                    }
                    else if (type instanceof DoubleType) {
                        initialValue = (rows + 50) % maxValue;
                        for (int j = 0; j < newRows; j++) {
                            values.add(initialValue + j);
                        }
                    }
                    else if (type instanceof DecimalType) {
                        initialValue = (rows + 60) % maxValue;
                        for (int j = 0; j < newRows; j++) {
                            values.add(initialValue + j);
                        }
                    }
                }
                Page page;
                if (isDictionaryBlocks) {
                    page = PageBuilderUtil.createPageWithDictionaryBlocks(buildTypes, PREFIX, columnValues);
                }
                else {
                    page = PageBuilderUtil.createPage(buildTypes, PREFIX, columnValues);
                }
                buildPages.add(page);
                rows += newRows;

                for (int i = 0; i < buildTypes.size(); i++) {
                    columnValues.get(i).clear();
                }
            }
            if (buildHashEnabled) {
                generateHashPage(buildPages, buildTypes, buildJoinChannels);
                buildHashChannel = OptionalInt.of(buildTypes.size());
            }
        }

        protected void generateHashPage(List<Page> pages, List<Type> inputTypes, List<Integer> hashChannels)
        {
            for (int i = 0; i < pages.size(); i++) {
                Page page = pages.get(i);
                pages.set(i, TypeUtils.getHashPage(page, inputTypes, hashChannels));
            }
        }
    }

    public static class JoinContext
            extends BuildContext
    {
        protected static final int PROBE_ROWS_NUMBER = 1_400_000;

        protected static final Map<String, ImmutableList<Type>> PROBE_TYPES = ImmutableMap
                .<String, ImmutableList<Type>>builder()
                .put("group1", ImmutableList.of(BIGINT, BIGINT, BIGINT, createVarcharType(30), createVarcharType(50)))
                .put("group2", ImmutableList.of(BIGINT, createVarcharType(10)))
                .put("group3",
                        ImmutableList.of(BIGINT, BIGINT, INTEGER, createVarcharType(50), INTEGER, INTEGER,
                                createVarcharType(50)))
                .put("group4", ImmutableList.of(BIGINT, BIGINT, INTEGER, INTEGER, INTEGER))
                .put("group5", ImmutableList.of(BIGINT, INTEGER))
                .put("group6", ImmutableList.of(createVarcharType(60), BIGINT))
                .put("group7", ImmutableList.of(BIGINT, BIGINT, BIGINT, INTEGER, createVarcharType(50), INTEGER, INTEGER,
                        createVarcharType(50)))
                .put("group8", ImmutableList.of(BIGINT, BIGINT, BIGINT, INTEGER))
                .put("group9", ImmutableList.of(BIGINT, BIGINT))
                .put("group10", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, INTEGER))
                .put("group11", ImmutableList.of(BIGINT))
                .put("group12",
                        ImmutableList.of(createVarcharType(50), DOUBLE, createVarcharType(50), BIGINT,
                                createVarcharType(50), BIGINT, createVarcharType(50), INTEGER, BIGINT))
                .put("group13", ImmutableList.of(createDecimalType(12, 2), BIGINT, BIGINT))
                .put("group14", ImmutableList.of(INTEGER, INTEGER, BIGINT)).build();

        protected static final Map<String, ImmutableList<Integer>> PROBE_OUTPUT_COLS = ImmutableMap
                .<String, ImmutableList<Integer>>builder().put("group1", ImmutableList.of(0, 2, 3, 4))
                .put("group2", ImmutableList.of()).put("group3", ImmutableList.of(0, 2, 3, 4, 5, 6))
                .put("group4", ImmutableList.of(0, 2, 3, 4)).put("group5", ImmutableList.of(1))
                .put("group6", ImmutableList.of(0)).put("group7", ImmutableList.of(0, 1, 3, 4, 5, 6, 7))
                .put("group8", ImmutableList.of(0, 2, 3)).put("group9", ImmutableList.of(0))
                .put("group10", ImmutableList.of(0, 1, 2, 3, 4, 6, 7)).put("group11", ImmutableList.of())
                .put("group12", ImmutableList.of(8, 2, 1, 4, 3, 7)).put("group13", ImmutableList.of(0, 2))
                .put("group14", ImmutableList.of(0, 1)).build();

        protected static final Map<String, ImmutableList<Integer>> PROBE_HASH_COLS = ImmutableMap
                .<String, ImmutableList<Integer>>builder().put("group1", ImmutableList.of(1))
                .put("group2", ImmutableList.of(0)).put("group3", ImmutableList.of(1))
                .put("group4", ImmutableList.of(1)).put("group5", ImmutableList.of(0))
                .put("group6", ImmutableList.of(1)).put("group7", ImmutableList.of(2))
                .put("group8", ImmutableList.of(1)).put("group9", ImmutableList.of(1))
                .put("group10", ImmutableList.of(5)).put("group11", ImmutableList.of(0))
                .put("group12", ImmutableList.of(2, 4, 6, 0, 5)).put("group13", ImmutableList.of(1))
                .put("group14", ImmutableList.of(2)).build();

        @Param({"0.1", "1", "2"})
        protected double matchRate = 1;

        protected List<Page> probePages = new ArrayList<>();
        protected List<Type> probeTypes;
        protected List<Integer> probeOutputChannels;
        protected List<Integer> probeJoinChannels;
        protected OptionalInt probeHashChannel;

        private DriverContext buildDriverContext;
        private Operator buildOperator;

        @Override
        protected void beforeSetupTrial()
        {
            super.beforeSetupTrial();
            probeTypes = PROBE_TYPES.get(testGroup);
            probeOutputChannels = PROBE_OUTPUT_COLS.get(testGroup);
            probeJoinChannels = PROBE_HASH_COLS.get(testGroup);
            probeHashChannel = OptionalInt.empty();
            if (buildHashEnabled) {
                probeHashChannel = OptionalInt.of(probeTypes.size());
            }
            initializeProbePages();
        }

        @Override
        protected void beforeSetupIteration()
        {
        }

        @Override
        protected void beforeCleanupTrial()
        {
            try {
                buildOperator.close();
            }
            catch (Exception e) {
                System.out.println(e);
            }
            buildDriverContext.finished();
            buildDriverContext.getPipelineContext().getTaskContext().getTaskStateMachine().finished();
        }

        @Override
        protected List<Page> buildPages()
        {
            return probePages;
        }

        @Override
        protected List<Page> forkPages(List<Page> pages)
        {
            List<Page> slicedPages = new ArrayList<>(pages.size());
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }

        public List<Type> getProbeTypes()
        {
            if (buildHashEnabled) {
                return ImmutableList.copyOf(Iterables.concat(probeTypes, ImmutableList.of(BIGINT)));
            }
            return probeTypes;
        }

        protected void initializeProbePages()
        {
            List<List<Integer>> columnValues = new ArrayList<>();
            for (int i = 0; i < probeTypes.size(); i++) {
                List<Integer> values = new ArrayList<>();
                columnValues.add(values);
            }

            List<Integer> initials = new ArrayList<>();
            Random random = new Random(42);
            int remainingRows = PROBE_ROWS_NUMBER;
            int rowsInPage = 0;
            while (remainingRows > 0) {
                double roll = random.nextDouble();

                for (int i = 0; i < probeTypes.size(); i++) {
                    Type type = probeTypes.get(i);
                    if (type instanceof VarcharType) {
                        initials.add(20 + remainingRows);
                    }
                    else if (type instanceof BigintType) {
                        initials.add(30 + remainingRows);
                    }
                    else if (type instanceof IntegerType) {
                        initials.add(40 + remainingRows);
                    }
                    else if (type instanceof DoubleType) {
                        initials.add(50 + remainingRows);
                    }
                    else if (type instanceof DecimalType) {
                        initials.add(60 + remainingRows);
                    }
                }

                int rowsCount = 1;
                if (matchRate < 1) {
                    // each row has matchRate chance to join
                    if (roll > matchRate) {
                        // generate not matched row
                        for (int i = 0; i < initials.size(); i++) {
                            initials.set(i, initials.get(i) * -1);
                        }
                    }
                }
                else if (matchRate > 1) {
                    // each row has will be repeated between one and 2*matchRate times
                    roll = roll * 2 * matchRate + 1;
                    // example for matchRate == 2:
                    // roll is within [0, 5) range
                    // rowsCount is within [0, 4] range, where each value has same probability
                    // so expected rowsCount is 2
                    rowsCount = (int) Math.floor(roll);
                }

                for (int i = 0; i < rowsCount; i++) {
                    if (rowsInPage >= ROWS_PER_PAGE) {
                        Page page;
                        if (isDictionaryBlocks) {
                            // create dictionary page
                            page = PageBuilderUtil.createPageWithDictionaryBlocks(probeTypes, PREFIX, columnValues);
                        }
                        else {
                            page = PageBuilderUtil.createPage(probeTypes, PREFIX, columnValues);
                        }
                        probePages.add(page);
                        rowsInPage = 0;

                        for (int j = 0; j < probeTypes.size(); j++) {
                            columnValues.get(j).clear();
                        }
                    }

                    for (int j = 0; j < probeTypes.size(); j++) {
                        columnValues.get(j).add(initials.get(j));
                    }
                    --remainingRows;
                    rowsInPage++;
                }
                initials.clear();
            }
            if (buildHashEnabled) {
                generateHashPage(probePages, probeTypes, probeJoinChannels);
            }
        }

        @Override
        protected TaskContext createTaskContext()
        {
            return createTaskContextBySizeInGigaByte(4);
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            HashBuilderOperatorFactory hashBuilderOperatorFactory = createBuildOperatorFactory();

            OperatorFactory operatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(HASH_JOIN_OPERATOR_ID, TEST_PLAN_NODE_ID, lookupSourceFactoryManager,
                    getProbeTypes(), probeJoinChannels, probeHashChannel,
                    Optional.of(probeOutputChannels), OptionalInt.empty(),
                    unsupportedPartitioningSpillerFactory());
            buildDriverContext = super.createTaskContext().addPipelineContext(0, true, true, false)
                    .addDriverContext();
            buildOperator = hashBuilderOperatorFactory.createOperator(buildDriverContext);
            buildHash(buildOperator, forkBuildPage(buildPages));
            finishBuildHash(lookupSourceFactoryManager);
            return operatorFactory;
        }
    }

    @Benchmark
    public JoinBridgeManager<PartitionedLookupSourceFactory> benchmarkBuildHash(BuildContext buildContext)
    {
        buildContext.buildHash(buildContext.createOperator(), buildContext.getRemainInputPages());
        return buildContext.lookupSourceFactoryManager;
    }

    @Benchmark
    public List<Page> benchmarkJoinHash(JoinContext joinContext)
    {
        return joinContext.doDefaultBenchMark();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashJoinOlkOperators.class.getSimpleName() + ".*").build();
        new Runner(options).run();
    }
}
