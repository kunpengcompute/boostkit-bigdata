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
import com.google.common.primitives.Ints;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DummySpillerFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.window.AggregateWindowFunction;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.operator.window.RankFunction;
import io.prestosql.operator.window.ReflectionWindowFunctionSupplier;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import nova.hetu.olk.operator.WindowOmniOperator;
import nova.hetu.olk.operator.benchmark.AbstractOperatorBenchmarkContext.AbstractOmniOperatorBenchmarkContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.TestWindowOperator.ROW_NUMBER;
import static io.prestosql.operator.WindowFunctionDefinition.window;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.prestosql.spi.sql.expression.Types.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.prestosql.spi.sql.expression.Types.WindowFrameType.RANGE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkWindowOmniOperator
{
    public static final int TOTAL_PAGES = 1;
    public static final int ROWS_PER_PAGE = 10000;

    public static final int NUMBER_OF_GROUP_COLUMNS = 2;

    private static final Metadata metadata = createTestMetadataManager();
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(),
            UNBOUNDED_FOLLOWING, Optional.empty());

    public static final List<WindowFunctionDefinition> RANK = ImmutableList
            .of(window(new ReflectionWindowFunctionSupplier<>("rank", BIGINT, ImmutableList.of(), RankFunction.class), BIGINT, UNBOUNDED_FRAME));
    public static final List<WindowFunctionDefinition> COUNT_BIGINT_GROUP2 = ImmutableList.of(window(
            AggregateWindowFunction.supplier(
                    new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE,
                            BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                    metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
                            new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE,
                                    BIGINT.getTypeSignature(), BIGINT.getTypeSignature()))),
            BIGINT, UNBOUNDED_FRAME, 2));
    public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP3 = ImmutableList
            .of(window(
                    AggregateWindowFunction.supplier(
                            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()),
                            metadata.getFunctionAndTypeManager()
                                    .getAggregateFunctionImplementation(new Signature(
                                            QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                            DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))),
                    BIGINT, UNBOUNDED_FRAME, 7));
    public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP5 = ImmutableList
            .of(window(
                    AggregateWindowFunction.supplier(
                            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()),
                            metadata.getFunctionAndTypeManager()
                                    .getAggregateFunctionImplementation(new Signature(
                                            QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                            DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))),
                    BIGINT, UNBOUNDED_FRAME, 1));
    public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP6 = ImmutableList
            .of(window(
                    AggregateWindowFunction.supplier(
                            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()),
                            metadata.getFunctionAndTypeManager()
                                    .getAggregateFunctionImplementation(new Signature(
                                            QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                            DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))),
                    BIGINT, UNBOUNDED_FRAME, 1));
    public static final List<WindowFunctionDefinition> AVG_BIGINT_GROUP7 = ImmutableList
            .of(window(
                    AggregateWindowFunction.supplier(
                            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()),
                            metadata.getFunctionAndTypeManager()
                                    .getAggregateFunctionImplementation(new Signature(
                                            QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE,
                                            DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()))),
                    BIGINT, UNBOUNDED_FRAME, 6));

    private static final Map<String, ImmutableList<Integer>> PARTITION_CHANNELS = ImmutableMap
            .<String, ImmutableList<Integer>>builder().put("group1", ImmutableList.of(0, 1))
            .put("group2", ImmutableList.of(0, 1, 2)).put("group3", ImmutableList.of(0, 1, 2, 3, 4))
            .put("group4", ImmutableList.of(0, 1, 2, 3)).put("group5", ImmutableList.of(1))
            .put("group6", ImmutableList.of(1)).put("group7", ImmutableList.of(0, 2, 3, 4)).build();
    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))
            .put("group2", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))
            .put("group3",
                    ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50),
                            createVarcharType(50), INTEGER, INTEGER, BIGINT, BIGINT))
            .put("group4",
                    ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50),
                            createVarcharType(50), INTEGER, INTEGER, BIGINT))
            .put("group5", ImmutableList.of(BIGINT, INTEGER)).put("group6", ImmutableList.of(BIGINT, INTEGER))
            .put("group7", ImmutableList.of(createVarcharType(50), createVarcharType(50), createVarcharType(50),
                    createVarcharType(50), createVarcharType(50), INTEGER, BIGINT))
            .build();
    private static final Map<String, List<WindowFunctionDefinition>> WINDOW_TYPES = ImmutableMap
            .<String, List<WindowFunctionDefinition>>builder().put("group1", ROW_NUMBER)
            .put("group2", COUNT_BIGINT_GROUP2).put("group3", AVG_BIGINT_GROUP3).put("group4", RANK)
            .put("group5", AVG_BIGINT_GROUP5).put("group6", AVG_BIGINT_GROUP6).put("group7", AVG_BIGINT_GROUP7)
            .build();
    private static final Map<String, List<Integer>> SORT_CHANNELS = ImmutableMap.<String, List<Integer>>builder()
            .put("group1", ImmutableList.of(3)).put("group2", ImmutableList.of(3)).put("group3", ImmutableList.of())
            .put("group4", ImmutableList.of(4, 5)).put("group5", ImmutableList.of())
            .put("group6", ImmutableList.of()).put("group7", ImmutableList.of()).build();

    @State(Thread)
    public static class Context
            extends AbstractOmniOperatorBenchmarkContext
    {
        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7"})
        String testGroup;

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        public int rowsPerPartition;

        @Param("0")
        public int numberOfPregroupedColumns;

        public int partitionsPerGroup;

        public static WindowOmniOperator.WindowOmniOperatorFactory createFactoryUnbounded(
                List<? extends Type> sourceTypes, List<Integer> outputChannels,
                List<WindowFunctionDefinition> functions, List<Integer> partitionChannels,
                List<Integer> preGroupedChannels, List<Integer> sortChannels, List<SortOrder> sortOrder,
                int preSortedChannelPrefix, SpillerFactory spillerFactory, boolean spillEnabled)
        {
            return new WindowOmniOperator.WindowOmniOperatorFactory(0, new PlanNodeId("test"), sourceTypes,
                    outputChannels, functions, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                    preSortedChannelPrefix, 10);
        }

        @Override
        protected List<Page> buildPages()
        {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            List<Page> pages = new ArrayList<>();
            for (int i = 0; i < TOTAL_PAGES; i++) {
                if (dictionaryBlocks) {
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(typesArray, ROWS_PER_PAGE));
                }
                else {
                    pages.add(PageBuilderUtil.createSequencePage(typesArray, ROWS_PER_PAGE));
                }
            }
            return pages;
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            List<Integer> partitionChannels = PARTITION_CHANNELS.get(testGroup);
            List<WindowFunctionDefinition> windowType = WINDOW_TYPES.get(testGroup);
            List<Type> inputTypes = INPUT_TYPES.get(testGroup);
            List<Integer> sortChannels = SORT_CHANNELS.get(testGroup);
            List<Integer> outputChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();

            for (int i = 0; i < inputTypes.size(); i++) {
                outputChannels.add(i);
            }

            for (int i = 0; i < sortChannels.size(); i++) {
                sortOrders.add(SortOrder.ASC_NULLS_LAST);
            }

            if (numberOfPregroupedColumns == 0) {
                //Ungrouped
                return createFactoryUnbounded(inputTypes, outputChannels, windowType, partitionChannels,
                        Ints.asList(), sortChannels, sortOrders, 0, new DummySpillerFactory(), false);
            }
            else if (numberOfPregroupedColumns < NUMBER_OF_GROUP_COLUMNS) {
                //Partially grouped
                return createFactoryUnbounded(inputTypes, outputChannels, windowType, partitionChannels,
                        Ints.asList(1), sortChannels, sortOrders, 0, new DummySpillerFactory(), false);
            }
            else {
                // Fully grouped and (potentially) sorted
                return createFactoryUnbounded(inputTypes, outputChannels, windowType, partitionChannels,
                        Ints.asList(0, 1), sortChannels, sortOrders,
                        (numberOfPregroupedColumns - NUMBER_OF_GROUP_COLUMNS), new DummySpillerFactory(), false);
            }
        }
    }

    @Benchmark
    public List<Page> benchmark(Context context)
    {
        return context.doDefaultBenchMark();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkWindowOmniOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
