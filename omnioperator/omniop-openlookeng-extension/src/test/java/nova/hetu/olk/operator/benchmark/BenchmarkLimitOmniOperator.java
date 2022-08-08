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
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.LimitOmniOperator;
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
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;

@State(Scope.Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkLimitOmniOperator
{
    public static final int TOTAL_PAGES = 1000;

    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(INTEGER))
            .put("group2", ImmutableList.of(createVarcharType(16))).put("group3", ImmutableList.of(DOUBLE))
            .put("group4", ImmutableList.of(createDecimalType()))
            .put("group5", ImmutableList.of(INTEGER, createVarcharType(16)))
            .put("group6", ImmutableList.of(INTEGER, BIGINT, createDecimalType(), DOUBLE))
            .put("group7", ImmutableList.of(createVarcharType(20), createVarcharType(30), createVarcharType(50)))
            .build();

    private static final Map<String, List<Integer>> SORT_CHANNELS = ImmutableMap.<String, List<Integer>>builder()
            .put("group1", ImmutableList.of(0)).put("group2", ImmutableList.of(0))
            .put("group3", ImmutableList.of(0)).put("group4", ImmutableList.of(0))
            .put("group5", ImmutableList.of(0, 1)).put("group6", ImmutableList.of(0, 1, 2, 3))
            .put("group7", ImmutableList.of(0, 1, 2)).build();

    @State(Scope.Thread)
    public static class BenchmarkContext
            extends AbstractOmniOperatorBenchmarkContext
    {
        @Param({"1", "100", "1000"})
        private String limit = "100";

        @Param({"group1", "group2", "group3"})
        String testGroup = "group1";

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Param({"32", "1024"})
        public String rowsPerPageStr = "1024";

        @Override
        protected List<Page> buildPages()
        {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            List<Page> pages = new ArrayList<>(TOTAL_PAGES);
            for (int i = 0; i < TOTAL_PAGES; i++) {
                if (dictionaryBlocks) {
                    pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(typesArray,
                            Integer.parseInt(rowsPerPageStr)));
                }
                else {
                    pages.add(PageBuilderUtil.createSequencePage(typesArray, Integer.parseInt(rowsPerPageStr)));
                }
            }
            return pages;
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            return new LimitOmniOperator.LimitOmniOperatorFactory(0, new PlanNodeId("test"),
                    Long.parseLong(limit), INPUT_TYPES.get(testGroup));
        }
    }

    @Benchmark
    public List<Page> limit(BenchmarkContext context)
    {
        return context.doDefaultBenchMark();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLimitOmniOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
