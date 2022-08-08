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
import io.prestosql.RowPagesBuilder;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.StreamingAggregationOperator.StreamingAggregationOperatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.gen.JoinCompiler;
import nova.hetu.olk.operator.HashAggregationOmniOperator;
import nova.hetu.olk.operator.benchmark.AbstractOperatorBenchmarkContext.AbstractOmniOperatorBenchmarkContext;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.constants.FunctionType;
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
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Scope.Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkHashAggregationOmniOperator
{
    private static final Metadata metadata = createTestMetadataManager();
    private static final VarcharType FIXED_WIDTH_VARCHAR = VarcharType.createVarcharType(200);

    private static final InternalAggregationFunction LONG_SUM = metadata.getFunctionAndTypeManager()
            .getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("sum"),
                    AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getFunctionAndTypeManager()
            .getAggregateFunctionImplementation(new Signature(QualifiedObjectName.valueOfDefaultFunction("count"),
                    AGGREGATE, BIGINT.getTypeSignature()));

    private static final Map<String, List<Type>> allTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2",
                    ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR,
                            INTEGER, INTEGER, BIGINT))
            .put("sql4", ImmutableList.of(FIXED_WIDTH_VARCHAR, INTEGER, INTEGER, INTEGER, BIGINT))
            .put("sql6", ImmutableList.of(INTEGER, INTEGER, BIGINT))
            .put("sql7",
                    ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, BIGINT, BIGINT,
                            BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT, BIGINT, FIXED_WIDTH_VARCHAR, BIGINT, BIGINT)).build();
    private static final Map<String, List<Integer>> channels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(0, 1, 2, 3, 4, 5, 6)).put("sql4", ImmutableList.of(0, 1, 2, 3, 4))
            .put("sql6", ImmutableList.of(0, 1, 2)).put("sql7", ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7))
            .put("sql9", ImmutableList.of(0, 1, 2, 3, 4, 5)).build();
    private static final Map<String, List<Integer>> hashChannels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(0, 1, 2, 3, 4, 5)).put("sql4", ImmutableList.of(0, 1, 2, 3))
            .put("sql6", ImmutableList.of(0, 1)).put("sql7", ImmutableList.of(0, 1, 2))
            .put("sql9", ImmutableList.of(0, 1, 2, 3)).build();
    private static final Map<String, List<Type>> hashTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2",
                    ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR,
                            INTEGER, INTEGER))
            .put("sql4", ImmutableList.of(FIXED_WIDTH_VARCHAR, INTEGER, INTEGER, INTEGER))
            .put("sql6", ImmutableList.of(INTEGER, INTEGER))
            .put("sql7", ImmutableList.of(FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR, FIXED_WIDTH_VARCHAR))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT, BIGINT, FIXED_WIDTH_VARCHAR)).build();
    private static final Map<String, List<Integer>> aggChannels = new ImmutableMap.Builder<String, List<Integer>>()
            .put("sql2", ImmutableList.of(6)).put("sql4", ImmutableList.of(4)).put("sql6", ImmutableList.of(2))
            .put("sql7", ImmutableList.of(3, 4, 5, 6, 7)).put("sql9", ImmutableList.of(4, 5)).build();
    private static final Map<String, List<Type>> aggInputTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(BIGINT)).put("sql4", ImmutableList.of(BIGINT))
            .put("sql6", ImmutableList.of(BIGINT)).put("sql7", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT)).build();
    private static final Map<String, List<Type>> aggOutputTypes = new ImmutableMap.Builder<String, List<Type>>()
            .put("sql2", ImmutableList.of(BIGINT)).put("sql4", ImmutableList.of(BIGINT))
            .put("sql6", ImmutableList.of(BIGINT)).put("sql7", ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, BIGINT))
            .put("sql9", ImmutableList.of(BIGINT, BIGINT)).build();
    private static final Map<String, List<String>> aggFuncTypes = new ImmutableMap.Builder<String, List<String>>()
            .put("sql2", ImmutableList.of("sum")).put("sql4", ImmutableList.of("sum"))
            .put("sql6", ImmutableList.of("sum")).put("sql7", ImmutableList.of("sum", "sum", "sum", "sum", "sum"))
            .put("sql9", ImmutableList.of("sum", "sum")).build();

    public static final int TOTAL_PAGES = 140;
    public static final int ROWS_PER_PAGE = 10_000;
    public static final String PREFIX_BASE = "A";

    @State(Thread)
    public static class Context
            extends AbstractOmniOperatorBenchmarkContext
    {
        @Param({"100", "1000", "10000"})
        public int rowsPerGroup = 100;

        @Param("hash")
        public String operatorType;

        @Param({"false", "true"})
        public boolean isDictionary;

        @Param({"0", "50", "150"})
        public int prefixLength;

        @Param({"sql2", "sql4", "sql6", "sql7", "sql9"})
        public static String sqlId = "sql2";

        private String genVarcharPrefix()
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < prefixLength; ++i) {
                stringBuilder.append(PREFIX_BASE);
            }
            return stringBuilder.toString();
        }

        @Override
        protected List<Page> buildPages()
        {
            List<Type> types = allTypes.get(sqlId);

            int groupsPerPage = ROWS_PER_PAGE / rowsPerGroup;
            boolean hashAggregation = operatorType.equalsIgnoreCase("hash");
            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(hashAggregation, hashChannels.get(sqlId),
                    hashTypes.get(sqlId));

            for (int i = 0; i < TOTAL_PAGES; i++) {
                List<Block<?>> allBlocks = new ArrayList<>();
                for (Type type : types) {
                    switch (type.getTypeSignature().getBase()) {
                        case "varchar":
                            allBlocks.add(createVarcharBlock(i, groupsPerPage));
                            break;
                        case "bigint":
                            allBlocks.add(createBigIntBlock(i, groupsPerPage));
                            break;
                        case "integer":
                            allBlocks.add(createIntegerBlock(i, groupsPerPage));
                            break;
                        default:
                            return null;
                    }
                }
                pagesBuilder.addBlocksPage(allBlocks.toArray(new Block[allBlocks.size()]));
            }
            return pagesBuilder.build();
        }

        private Block<?> createVarcharBlock(int pageId, int groupsPerPage)
        {
            String prefix = genVarcharPrefix();
            if (!isDictionary) {
                BlockBuilder<?> blockBuilder = VARCHAR.createBlockBuilder(null, ROWS_PER_PAGE, 200);
                for (int k = 0; k < groupsPerPage; k++) {
                    String groupKey = format(prefix + "%s", pageId * groupsPerPage + k);
                    repeatToStringBlock(groupKey, rowsPerGroup, blockBuilder);
                }
                return blockBuilder.build();
            }
            else {
                return createVarcharDictionary(pageId, prefix, groupsPerPage);
            }
        }

        private Block<?> createBigIntBlock(int pageId, int groupsPerPage)
        {
            if (!isDictionary) {
                BlockBuilder<?> blockBuilder = BIGINT.createBlockBuilder(null, ROWS_PER_PAGE);
                for (int k = 0; k < groupsPerPage; k++) {
                    long groupKey = (long) pageId * groupsPerPage + k;
                    repeatToLongBlock(groupKey, rowsPerGroup, blockBuilder);
                }
                return blockBuilder.build();
            }
            else {
                return createLongDictionary(pageId, groupsPerPage);
            }
        }

        private Block<?> createIntegerBlock(int pageId, int groupsPerPage)
        {
            if (!isDictionary) {
                BlockBuilder<?> blockBuilder = INTEGER.createBlockBuilder(null, ROWS_PER_PAGE);
                for (int k = 0; k < groupsPerPage; k++) {
                    long groupKey = (long) pageId * groupsPerPage + k;
                    repeatToIntegerBlock(groupKey, rowsPerGroup, blockBuilder);
                }
                return blockBuilder.build();
            }
            else {
                return createIntegerDictionary(pageId, groupsPerPage);
            }
        }

        private FunctionType[] transferAggType(List<String> aggregators)
        {
            FunctionType[] res = new FunctionType[aggregators.size()];
            for (int i = 0; i < aggregators.size(); i++) {
                // aggregator type, eg:sum,avg...
                String agg = aggregators.get(i);
                switch (agg) {
                    case "sum":
                        res[i] = OMNI_AGGREGATION_TYPE_SUM;
                        break;
                    case "avg":
                        res[i] = OMNI_AGGREGATION_TYPE_AVG;
                        break;
                    case "count":
                        res[i] = OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
                        break;
                    case "min":
                        res[i] = OMNI_AGGREGATION_TYPE_MIN;
                        break;
                    case "max":
                        res[i] = OMNI_AGGREGATION_TYPE_MAX;
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported Aggregator type by OmniRuntime: " + agg);
                }
            }
            return res;
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            boolean hashAggregation = operatorType.equalsIgnoreCase("hash");

            if (hashAggregation) {
                return createHashAggregationOperatorFactory();
            }
            else {
                return createStreamingAggregationOperatorFactory();
            }
        }

        private OperatorFactory createStreamingAggregationOperatorFactory()
        {
            return new StreamingAggregationOperatorFactory(0, new PlanNodeId("test"),
                    ImmutableList.of(VARCHAR), ImmutableList.of(VARCHAR), ImmutableList.of(0),
                    AggregationNode.Step.SINGLE,
                    ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                            LONG_SUM.bind(ImmutableList.of(1), Optional.empty())),
                    new JoinCompiler(createTestMetadataManager()));
        }

        private OperatorFactory createHashAggregationOperatorFactory()
        {
            ImmutableList.Builder<Optional<Integer>> maskChannels = ImmutableList.builder();
            for (int i = 0; i < aggFuncTypes.get(sqlId).size(); i++) { // one mask channel for each agg func
                maskChannels.add(Optional.empty());
            }
            return new HashAggregationOmniOperator.HashAggregationOmniOperatorFactory(0, new PlanNodeId("test"),
                    allTypes.get(sqlId), Ints.toArray(hashChannels.get(sqlId)),
                    OperatorUtils.toDataTypes(hashTypes.get(sqlId)), Ints.toArray(aggChannels.get(sqlId)),
                    OperatorUtils.toDataTypes(aggInputTypes.get(sqlId)), transferAggType(aggFuncTypes.get(sqlId)),
                    maskChannels.build(), OperatorUtils.toDataTypes(aggOutputTypes.get(sqlId)),
                    AggregationNode.Step.SINGLE);
        }

        private static void repeatToStringBlock(String value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                VARCHAR.writeString(blockBuilder, value);
            }
        }

        private static void repeatToLongBlock(long value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                BIGINT.writeLong(blockBuilder, value);
            }
        }

        private static void repeatToIntegerBlock(long value, int count, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < count; i++) {
                INTEGER.writeLong(blockBuilder, value);
            }
        }

        private static Block createVarcharDictionary(int pageId, String prefix, int groupCount)
        {
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                String groupKey = format(prefix + "%s", pageId * groupCount + k);
                VARCHAR.writeString(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
        }

        private static Block createLongDictionary(int pageId, int groupCount)
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                long groupKey = pageId * groupCount + k;
                BIGINT.writeLong(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
        }

        private static Block createIntegerDictionary(int pageId, int groupCount)
        {
            BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, groupCount);
            for (int k = 0; k < groupCount; k++) {
                long groupKey = pageId * groupCount + k;
                INTEGER.writeLong(blockBuilder, groupKey);
            }
            int[] ids = new int[ROWS_PER_PAGE];
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                ids[k] = k % groupCount;
            }
            return new DictionaryBlock(blockBuilder.build(), ids);
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
                .include(".*" + BenchmarkHashAggregationOmniOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
