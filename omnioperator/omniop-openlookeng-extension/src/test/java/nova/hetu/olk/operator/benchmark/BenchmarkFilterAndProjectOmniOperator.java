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
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.testing.TestingSession;
import nova.hetu.olk.operator.benchmark.AbstractOperatorBenchmarkContext.AbstractOmniOperatorBenchmarkContext;
import nova.hetu.olk.operator.filterandproject.FilterAndProjectOmniOperator;
import nova.hetu.olk.operator.filterandproject.OmniExpressionCompiler;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.FunctionAssertions.createExpression;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Locale.ENGLISH;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFilterAndProjectOmniOperator
{
    private static final VarcharType VARCHAR = VarcharType.createVarcharType(200);
    private static final CharType CHAR = CharType.createCharType(200);
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final TypeAnalyzer TYPE_ANALYZER = new TypeAnalyzer(new SqlParser(), METADATA);

    private static final int TOTAL_POSITIONS = 1_000_000;
    private static final DataSize FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = new DataSize(500, KILOBYTE);
    private static final int FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = 256;

    static PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(METADATA, 0);
    static OmniExpressionCompiler omniExpressionCompiler = new OmniExpressionCompiler(METADATA, pageFunctionCompiler);

    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("q1", ImmutableList.of(INTEGER, INTEGER, INTEGER, DATE))
            .put("q2", ImmutableList.of(BIGINT, BIGINT, INTEGER, INTEGER))
            .put("q3", ImmutableList.of(VARCHAR, VARCHAR, INTEGER))
            .put("q4", ImmutableList.of(VARCHAR, INTEGER, INTEGER))
            .put("q5", ImmutableList.of(CHAR, INTEGER, INTEGER))
            .put("q6", ImmutableList.of(VARCHAR, BIGINT, INTEGER))
            .put("q7", ImmutableList.of(VARCHAR, INTEGER))
            .put("q8", ImmutableList.of(VARCHAR, VARCHAR, BIGINT, INTEGER))
            .put("q9", ImmutableList.of(BIGINT, INTEGER, INTEGER, VARCHAR))
            .put("q10", ImmutableList.of(BIGINT, INTEGER, INTEGER, VARCHAR)).build();

    static Map<String, Map<Symbol, Type>> symbolTypes = new HashMap<>();

    static Map<String, Map<Symbol, Integer>> sourceLayout = new HashMap<>();

    static {
        for (Entry<String, ImmutableList<Type>> entry : INPUT_TYPES.entrySet()) {
            List<Type> types = entry.getValue();
            Map<Symbol, Type> symbolTypes = new HashMap<>();
            Map<Symbol, Integer> sourceLayout = new HashMap<>();
            for (int i = 0; i < types.size(); i++) {
                Symbol symbol = new Symbol(types.get(i).getTypeSignature().getBase().toLowerCase(ENGLISH) + i);
                symbolTypes.put(symbol, types.get(i));
                sourceLayout.put(symbol, i);
            }
            BenchmarkFilterAndProjectOmniOperator.symbolTypes.put(entry.getKey(), symbolTypes);
            BenchmarkFilterAndProjectOmniOperator.sourceLayout.put(entry.getKey(), sourceLayout);
        }
    }

    @State(Thread)
    public static class Context
            extends AbstractOmniOperatorBenchmarkContext
    {
        @Param({"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"})
        String query;

        @Param({"32", "1024"})
        int positionsPerPage = 32;

        @Param({"false", "true"})
        boolean dictionaryBlocks;

        @Override
        protected List<Page> buildPages()
        {
            return buildPages(INPUT_TYPES.get(this.query), TOTAL_POSITIONS / positionsPerPage,
                    positionsPerPage, dictionaryBlocks);
        }

        @Override
        protected OperatorFactory createOperatorFactory()
        {
            List<Type> types = INPUT_TYPES.get(this.query);

            List<RowExpression> projections = getProjections();
            PageProcessor pageProcessor = omniExpressionCompiler
                    .compilePageProcessor(Optional.of(getFilter()), projections, Optional.empty(), OptionalInt.empty(),
                            types, new TaskId("test")).get();
            return new FilterAndProjectOmniOperator.FilterAndProjectOmniOperatorFactory(0,
                    new PlanNodeId("test"), () -> pageProcessor, types, FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                    FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT, types);
        }

        private RowExpression getFilter()
        {
            switch (query) {
                case "q1":
                    return rowExpression("integer0 in (1,2) or (integer1 between 1 and 10) or integer2 in (0,1,2,3)");
                case "q2":
                    return rowExpression("bigint0 > 0 or integer2 = 10 or (integer3 in (1,2) or integer3 = 3)");
                case "q3":
                    return rowExpression(
                            "varchar0 in ('1','2','3','4','5','6','7','8','9','10') or varchar1 in ('1','2') or integer2 in (1,2,3,4,5)");
                case "q4":
                    return rowExpression("varchar0 in ('1','2','3') or integer1 = 3 or integer2 = 3");
                case "q5":
                    return rowExpression("char0 = '3' or integer1 >= 10 or integer2 <= 20");
                case "q6":
                    return rowExpression(
                            "varchar0 in ('1','2','3','4','5','6','7','8','9','10') or (bigint1 between 1 and 10) or integer2 in (1,2,3,4,5)");
                case "q7":
                    return rowExpression("integer1 between 3 and 5");
                case "q8":
                    return rowExpression(
                            "varchar0 in ('1','2','3','4','5','6','7','8','9','10') or bigint2 between 3 and 5 or integer3 = 3");
                case "q9":
                    return rowExpression("bigint0 between 3 and 5 or integer1 = 3 or integer2 in (1,2)");
                case "q10":
                    return rowExpression("bigint0 between 3 and 5 or integer1 = 3 or integer2 = 3");
                default:
                    throw new IllegalArgumentException("Unsupported query!");
            }
        }

        private List<RowExpression> getProjections()
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();

            switch (query) {
                case "q1": {
                    builder.add(rowExpression("integer0"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("date3"));
                    break;
                }
                case "q2": {
                    builder.add(rowExpression("bigint0 - 1"));
                    builder.add(rowExpression("bigint1 + 1"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("cast(integer3 as BIGINT)"));
                    break;
                }
                case "q3": {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("varchar1"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    break;
                }
                case "q4": {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    break;
                }
                case "q5": {
                    // 'concat' function lead to MEMORY_LEAK
                    builder.add(rowExpression("concat(concat('foo', char0), 'lish')"));
                    builder.add(rowExpression("integer1"));
                    builder.add(rowExpression("integer2"));
                    break;
                }
                case "q6": {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("bigint1"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    break;
                }
                case "q7": {
                    builder.add(rowExpression("substr(varchar0, 0, 1)"));
                    builder.add(rowExpression("integer1"));
                    break;
                }
                case "q8": {
                    builder.add(rowExpression("varchar0"));
                    builder.add(rowExpression("varchar1"));
                    builder.add(rowExpression("bigint2"));
                    builder.add(rowExpression("cast(integer3 as BIGINT)"));
                    break;
                }
                case "q9": {
                    builder.add(rowExpression("bigint0"));
                    builder.add(rowExpression("cast(integer1 as BIGINT)"));
                    builder.add(rowExpression("cast(integer2 as BIGINT)"));
                    builder.add(rowExpression("substr(varchar3, 0, 1)"));
                    break;
                }
                case "q10": {
                    builder.add(rowExpression("bigint0"));
                    builder.add(rowExpression("cast(integer1 as BIGINT)"));
                    builder.add(rowExpression("integer2"));
                    builder.add(rowExpression("substr(varchar3, 0, 1)"));
                    break;
                }
                default:
                    break;
            }
            return builder.build();
        }

        private RowExpression rowExpression(String value)
        {
            Expression expression = createExpression(value, METADATA, TypeProvider.copyOf(symbolTypes.get(query)));

            return SqlToRowExpressionTranslator.translate(expression, SCALAR,
                    TYPE_ANALYZER.getTypes(TEST_SESSION, TypeProvider.copyOf(symbolTypes.get(query)), expression), sourceLayout.get(query),
                    METADATA.getFunctionAndTypeManager(), TEST_SESSION, true);
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
                .include(".*" + BenchmarkFilterAndProjectOmniOperator.class.getSimpleName() + ".*").build();

        new Runner(options).run();
    }
}
