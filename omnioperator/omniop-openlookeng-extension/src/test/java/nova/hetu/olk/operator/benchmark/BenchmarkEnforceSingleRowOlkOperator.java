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
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.EnforceSingleRowOperator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingSnapshotUtils;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class BenchmarkEnforceSingleRowOlkOperator
{
    private BenchmarkEnforceSingleRowOlkOperator()
    {
    }

    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("boolean", ImmutableList.of(BOOLEAN))
            .put("long", ImmutableList.of(BIGINT))
            .put("decimal", ImmutableList.of(createDecimalType())).put("date", ImmutableList.of(DATE))
            .put("double", ImmutableList.of(DOUBLE)).put("varchar", ImmutableList.of(createVarcharType(50)))
            .build();

    @State(Scope.Thread)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    @Fork(1)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    public static class BenchmarkContext
    {
        private ExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private TaskContext taskContext;
        private OperatorFactory factory;

        @Param({"long", "boolean", "double", "decimal", "date", "varchar"})
        String testGroup = "long";

        @Setup
        public void setUp()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);
            factory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(0,
                    new PlanNodeId("plan-node-enforceSingleRow"));
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
            try {
                Class cl = TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS.getClass();
                Field field = cl.getDeclaredField("deleteSnapshotExecutor");
                field.setAccessible(true);
                ScheduledThreadPoolExecutor snapshotExecutor = (ScheduledThreadPoolExecutor) field.get(TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS);
                snapshotExecutor.shutdownNow();
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                System.out.println(e);
            }
        }

        @Benchmark
        public void testEnforceSingleRowOlkOperator(Blackhole blackhole)
        {
            DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
            EnforceSingleRowOperator operator = (EnforceSingleRowOperator) factory.createOperator(driverContext);
            List<Page> input = buildPages(testGroup);
            operator.addInput(input.get(0));

            try {
                operator.addInput(input.get(1));
            }
            catch (PrestoException e) {
                System.out.println(e);
            }
            operator.finish();
            operator.close();
            blackhole.consume(input);
        }
    }

    public static List<Page> buildPages(String testGroup)
    {
        return rowPagesBuilder(INPUT_TYPES.get(testGroup)).addSequencePage(1, 1)
                .addSequencePage(2, 1).build();
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include("nova.hetu.olk.operator.benchmark." + BenchmarkEnforceSingleRowOlkOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
