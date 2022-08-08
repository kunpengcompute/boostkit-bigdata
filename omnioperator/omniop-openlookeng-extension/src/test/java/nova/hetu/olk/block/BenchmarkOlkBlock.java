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

package nova.hetu.olk.block;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.benchmark.PageBuilderUtil;
import nova.hetu.olk.tool.BlockUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkOlkBlock
{
    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("long", ImmutableList.of(BIGINT))
            .put("int", ImmutableList.of(INTEGER)).put("double", ImmutableList.of(DOUBLE))
            .put("varchar", ImmutableList.of(createVarcharType(50))).build();

    private static final int TOTAL_PAGES = 100;
    private static final int ROWS_PER_PAGE = 10000;
    private static final int[] POSITIONS;

    static {
        POSITIONS = new int[ROWS_PER_PAGE / 2];
        for (int i = 0; i < POSITIONS.length; i++) {
            POSITIONS[i] = i * 2;
        }
    }

    @State(Thread)
    public static class Context
    {
        @Param({"int", "long", "double", "varchar"})
        String dataType;

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        private List<Page> pages;

        private List<Block<?>> newBlocks;

        @Setup(Invocation)
        public void setup()
        {
            pages = generateTestData();
            newBlocks = new LinkedList<>();
        }

        @TearDown(Invocation)
        public void cleanup()
        {
            for (Page page : pages) {
                BlockUtils.freePage(page);
            }

            for (Block<?> block : newBlocks) {
                block.close();
            }
        }

        private List<Page> generateTestData()
        {
            List<Type> typesArray = INPUT_TYPES.get(dataType);
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

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Benchmark
    public List<Block<?>> blockCopyPositions(Context context)
    {
        for (Page page : context.getPages()) {
            int positionCount = page.getPositionCount();
            int channelCount = page.getChannelCount();
            for (int i = 0; i < channelCount; i++) {
                Block<?> block = page.getBlock(i).copyPositions(POSITIONS, 0, positionCount / 2);
                context.newBlocks.add(block);
            }
        }
        return context.newBlocks;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkOlkBlock.class.getSimpleName() + ".*").build();
        new Runner(options).run();
    }
}
