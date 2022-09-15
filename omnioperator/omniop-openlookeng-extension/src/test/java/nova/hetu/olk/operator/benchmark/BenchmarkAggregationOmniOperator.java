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
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.AggregationOmniOperator;
import nova.hetu.olk.operator.benchmark.AbstractOperatorBenchmarkContext.AbstractOmniOperatorBenchmarkContext;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.LongDataType;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;

@State(Scope.Thread)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkAggregationOmniOperator
{
    public static final int TOTAL_PAGES = 100;
    public static final int ROWS_PER_PAGE = 10000;

    // thus varchar, boolean, decimal cannot aggregation of SUM, AVG
    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("double", ImmutableList.of(DOUBLE))
            .put("long", ImmutableList.of(BIGINT))
            .build();

    private static final Map<String, FunctionType[]> AGG_TYPES = ImmutableMap
            .<String, FunctionType[]>builder().put("SUM", new FunctionType[]{OMNI_AGGREGATION_TYPE_SUM})
            .put("AVG", new FunctionType[]{OMNI_AGGREGATION_TYPE_AVG})
            .put("MAX", new FunctionType[]{OMNI_AGGREGATION_TYPE_MAX})
            .put("MIN", new FunctionType[]{OMNI_AGGREGATION_TYPE_MIN})
            .put("COUNT_COLUMN", new FunctionType[]{OMNI_AGGREGATION_TYPE_COUNT_COLUMN})
            .put("COUNT_ALL", new FunctionType[]{OMNI_AGGREGATION_TYPE_COUNT_ALL})
            .build();

    private static final DataType[] doubleType = new DataType[]{DoubleDataType.DOUBLE};
    private static final DataType[] longType = new DataType[]{LongDataType.LONG};
    private static ImmutableList.Builder<Optional<Integer>> maskChannels = new ImmutableList.Builder<>();

    static {
        maskChannels.add(Optional.empty());
    }

    private static DataType[] getAggOutput(String aggType, String group)
    {
        switch (aggType) {
            case "COUNT_COLUMN":
            case "COUNT_ALL":
                return longType;
            case "MAX":
            case "MIN":
            case "SUM":
                if (group.equals("long")) {
                    return longType;
                }
                else {
                    return doubleType;
                }
            default:
                return doubleType;
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkContext
            extends AbstractOmniOperatorBenchmarkContext
    {
        @Param({"SUM", "AVG", "MAX", "MIN", "COUNT_COLUMN", "COUNT_ALL"})
        private String aggType = "AVG";

        @Param({"double", "long"})
        String testGroup = "long";

        @Override
        protected List<Page> buildPages()
        {
            return buildPages(INPUT_TYPES.get(testGroup), TOTAL_PAGES, ROWS_PER_PAGE, false);
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            return new AggregationOmniOperator.AggregationOmniOperatorFactory(0,
                    new PlanNodeId("test"), INPUT_TYPES.get(testGroup), AGG_TYPES.get(aggType), new int[]{0},
                    maskChannels.build(), getAggOutput(aggType, testGroup), AggregationNode.Step.SINGLE);
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
                .include(".*" + BenchmarkAggregationOmniOperator.class.getSimpleName() + ".*").build();
        new Runner(options).run();
    }
}
