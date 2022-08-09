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
import io.prestosql.SessionTestUtils;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import nova.hetu.olk.tool.BlockUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static org.openjdk.jmh.annotations.Level.Iteration;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@Fork(0)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkOlkFilterAndProject
{
    private static final int SHIP_DATE = 0;
    private static final int EXTENDED_PRICE = 1;
    private static final int DISCOUNT = 2;
    private static final int QUANTITY = 3;
    private static final long CONDITION = 10471;
    private static final int PAGE_SIZE = 1024;
    private static final int PAGE_COUNT = 1000;
    private static final List<Type> INPUT_TYPES = ImmutableList.of(INTEGER, BIGINT, BIGINT, BIGINT);
    private static final RowExpression FILTER_FOR_Q1_OMNI_FILTER = call(
            internalOperator(OperatorType.LESS_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), INTEGER.getTypeSignature(),
                    INTEGER.getTypeSignature()),
            BOOLEAN, field(SHIP_DATE, BIGINT), constant(CONDITION, BIGINT));
    private static final RowExpression PROJECT = call(
            internalOperator(OperatorType.MULTIPLY, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(),
                    BIGINT.getTypeSignature()),
            BIGINT, field(EXTENDED_PRICE, BIGINT), field(DISCOUNT, BIGINT));
    private static final Metadata METADATA = createTestMetadataManager();
    private static final PageProcessor COMPILED_PROCESSOR = getCompiledProcessor();

    @State(Scope.Thread)
    public static class Context
    {
        @Param({"0.2", "0.4", "0.6", "0.8"})
        float selectedRatio = 0.2f;

        private List<Page> inputPages;

        private List<Iterator<Optional<Page>>> result = new LinkedList<>();

        @Setup(Iteration)
        public void setup()
        {
            inputPages = createInputPages(PAGE_SIZE, PAGE_COUNT, selectedRatio, CONDITION);
        }

        @TearDown(Iteration)
        public void cleanup()
        {
            for (Iterator<Optional<Page>> pageIterator : result) {
                while (pageIterator.hasNext()) {
                    Optional<Page> page = pageIterator.next();
                    page.ifPresent(BlockUtils::freePage);
                }
            }
            result = new LinkedList<>();
        }
    }

    @Benchmark
    public List<Optional<Page>> compileWithFilterAndProject(Context context)
    {
        for (Page input : context.inputPages) {
            Iterator<Optional<Page>> iterator = COMPILED_PROCESSOR.process(
                    SessionTestUtils.TEST_SESSION.toConnectorSession(), new DriverYieldSignal(),
                    newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                    input);
            context.result.add(iterator);
        }
        return ImmutableList.of();
    }

    private static PageProcessor getCompiledProcessor()
    {
        return new ExpressionCompiler(METADATA, new PageFunctionCompiler(METADATA, 10_000))
                .compilePageProcessor(Optional.of(FILTER_FOR_Q1_OMNI_FILTER), ImmutableList.of(PROJECT)).get();
    }

    private static RowExpression call(Signature signature, Type type, RowExpression... arguments)
    {
        BuiltInFunctionHandle functionHandle = new BuiltInFunctionHandle(signature);
        return new CallExpression(signature.getName().getObjectName(), functionHandle, type,
                Arrays.asList(arguments));
    }

    private static List<Page> createInputPages(int pageSize, int pageCount, float selectedRatio, long condition)
    {
        List<Page> pageContainer = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            pageContainer.add(createInputPages(pageSize, selectedRatio, condition));
        }
        return pageContainer;
    }

    private static Page createInputPages(int pageSize, float selectedRatio, long condition)
    {
        PageBuilder pageBuilder = new PageBuilder(INPUT_TYPES);
        for (int j = 0; j < pageSize; j++) {
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(EXTENDED_PRICE), j);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(DISCOUNT), j);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(QUANTITY), j);
        }
        int selectedCount = (int) (selectedRatio * pageSize);
        for (int i = 0; i < selectedCount; i++) {
            INTEGER.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), condition);
        }
        for (int i = selectedCount; i < pageSize; i++) {
            INTEGER.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), condition + 1);
        }
        return pageBuilder.build();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkOlkFilterAndProject.class.getSimpleName() + ".*").build();
        new Runner(options).run();
    }
}
