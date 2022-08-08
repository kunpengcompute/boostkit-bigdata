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
import io.prestosql.operator.TopNOperator;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
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
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;

@State(Scope.Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkTopNOlkOperator
{
    public static final int TOTAL_PAGES = 1000;

    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(createVarcharType(16)))
            .put("group2", ImmutableList.of(INTEGER, INTEGER))
            .put("group3", ImmutableList.of(INTEGER, INTEGER, INTEGER))
            .put("group4", ImmutableList.of(BIGINT, INTEGER)).put("group5", ImmutableList.of(createVarcharType(16)))
            .put("group6", ImmutableList.of(INTEGER, INTEGER, INTEGER))
            .put("group7", ImmutableList.of(createVarcharType(20), createVarcharType(30), createVarcharType(50)))
            .put("group8", ImmutableList.of(createVarcharType(50), INTEGER))
            .put("group9",
                    ImmutableList.of(INTEGER, createVarcharType(60), createVarcharType(20), createVarcharType(30)))
            .put("group10",
                    ImmutableList.of(INTEGER, createVarcharType(50), INTEGER, INTEGER, createVarcharType(50)))
            .build();

    private static final Map<String, List<Integer>> SORT_CHANNELS = ImmutableMap.<String, List<Integer>>builder()
            .put("group1", ImmutableList.of(0)).put("group2", ImmutableList.of(0, 1))
            .put("group3", ImmutableList.of(0, 1, 2)).put("group4", ImmutableList.of(0, 1))
            .put("group5", ImmutableList.of(0)).put("group6", ImmutableList.of(0, 1, 2))
            .put("group7", ImmutableList.of(0, 1, 2)).put("group8", ImmutableList.of(0, 1))
            .put("group9", ImmutableList.of(0, 1, 2, 3)).put("group10", ImmutableList.of(0, 1, 2, 3)).build();

    @State(Scope.Thread)
    public static class BenchmarkContext
            extends AbstractOlkOperatorBenchmarkContext
    {
        @Param({"1", "10", "100", "1000", "10000"})
        private String topN = "100";

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7", "group8", "group9", "group10"})
        String testGroup = "group1";

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Param({"32", "1024"})
        public String rowsPerPageStr = "1024";

        private TopNOperator.TopNOperatorFactory createFactoryUnbounded(List<? extends Type> sourceTypes,
                                                                        List<Integer> sortChannels, List<SortOrder> sortOrder)
        {
            return new TopNOperator.TopNOperatorFactory(0, new PlanNodeId("test"), sourceTypes, Integer.valueOf(topN),
                    sortChannels, sortOrder);
        }

        @Override
        protected List<Page> buildPages()
        {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            List<Page> pages = new ArrayList<>();
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
            List<Type> inputTypes = INPUT_TYPES.get(testGroup);
            List<Integer> sortChannels = SORT_CHANNELS.get(testGroup);
            List<SortOrder> sortOrders = new ArrayList<>();
            for (int i = 0; i < sortChannels.size(); i++) {
                sortOrders.add(SortOrder.ASC_NULLS_LAST);
            }
            //Ungrouped
            return createFactoryUnbounded(inputTypes, sortChannels, sortOrders);
        }
    }

    @Benchmark
    public List<Page> topN(BenchmarkContext context)
    {
        return context.doDefaultBenchMark();
    }

    public static void main(String[] args) throws RunnerException
    {
        BenchmarkContext data = new BenchmarkContext();
        data.setup();
        new BenchmarkTopNOlkOperator().topN(data);

        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkTopNOlkOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
