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
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.AggregationOperator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.benchmark.AbstractOperatorBenchmarkContext.AbstractOlkOperatorBenchmarkContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

@State(Scope.Thread)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkAggregationOlkOperator
{
    public static final int TOTAL_PAGES = 100;
    public static final int ROWS_PER_PAGE = 10000;

    private static final Metadata TEST_METADATA_MANAGER = MetadataManager.createTestMetadataManager();

    // thus varchar, boolean, decimal cannot aggregation of SUM, AVG
    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("double", ImmutableList.of(DOUBLE))
            .put("long", ImmutableList.of(BIGINT))
            .build();

    private static ImmutableList.Builder<Optional<Integer>> maskChannels = new ImmutableList.Builder<>();

    static {
        maskChannels.add(Optional.empty());
    }

    private static final InternalAggregationFunction LONG_AVERAGE = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_AVERAGE = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("avg"), AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_SUM = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("sum"), AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("sum"), AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("count"), AGGREGATE, BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_MAX = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_MIN = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("min"), AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_MAX = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("max"), AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_MIN = TEST_METADATA_MANAGER.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("min"), AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static List<AccumulatorFactory> getFactory(String aggType, String group)
    {
        switch (aggType){
            case "AVG":
                if (group.equals("long")) {
                    return ImmutableList.of(LONG_AVERAGE.bind(ImmutableList.of(0), Optional.empty()));
                }
                else {
                    return ImmutableList.of(DOUBLE_AVERAGE.bind(ImmutableList.of(0), Optional.empty()));
                }
            case "SUM":
                if (group.equals("long")) {
                    return ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty()));
                }
                else {
                    return ImmutableList.of(DOUBLE_SUM.bind(ImmutableList.of(0), Optional.empty()));
                }
            case "MIN":
                if (group.equals("long")) {
                    return ImmutableList.of(LONG_MIN.bind(ImmutableList.of(0), Optional.empty()));
                }
                else {
                    return ImmutableList.of(DOUBLE_MIN.bind(ImmutableList.of(0), Optional.empty()));
                }
            case "MAX":
                if (group.equals("long")) {
                    return ImmutableList.of(LONG_MAX.bind(ImmutableList.of(0), Optional.empty()));
                }
                else {
                    return ImmutableList.of(DOUBLE_MAX.bind(ImmutableList.of(0), Optional.empty()));
                }
            default:
                return ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()));
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkContext
            extends AbstractOlkOperatorBenchmarkContext
    {
        @Param({"AVG", "MAX", "MIN", "SUM", "COUNT_COLUMN", "COUNT_ALL"})
        private String aggType = "AVG";

        @Param({"double", "long"})
        String testGroup = "double";

        @Override
        protected List<Page> buildPages()
        {
            return buildPages(INPUT_TYPES.get(testGroup), TOTAL_PAGES, ROWS_PER_PAGE, false);
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            return new AggregationOperator.AggregationOperatorFactory(0,
                    new PlanNodeId("test"), AggregationNode.Step.SINGLE,
                    getFactory(aggType, testGroup), false);
        }
    }

    @Benchmark
    public List<Page> aggregation(BenchmarkContext context)
    {
        return context.doDefaultBenchMark();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkAggregationOlkOperator.class.getSimpleName() + ".*")
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(options).run();
    }
}
