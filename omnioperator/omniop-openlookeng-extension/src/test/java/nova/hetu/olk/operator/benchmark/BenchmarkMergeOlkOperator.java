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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStateMachine;
import io.prestosql.failuredetector.NoOpFailureDetector;
import io.prestosql.metadata.Split;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.ExchangeClientConfig;
import io.prestosql.operator.ExchangeClientFactory;
import io.prestosql.operator.ExchangeOperator;
import io.prestosql.operator.MergeOperator;
import io.prestosql.operator.MergeOperator.MergeOperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TestingExchangeHttpClientHandler;
import io.prestosql.operator.TestingTaskBuffer;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.testing.TestingSnapshotUtils;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.BlockUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_FIRST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@State(Scope.Thread)
@Fork(1)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkMergeOlkOperator
{
    public static final int TOTAL_PAGES = 100;
    public static final int ROWS_PER_PAGE = 10000;
    private static final String TASK = "task";

    private static AtomicInteger operatorId = new AtomicInteger();
    private static final Map<String, ImmutableList<Type>> INPUT_TYPES = ImmutableMap
            .<String, ImmutableList<Type>>builder().put("group1", ImmutableList.of(INTEGER, INTEGER, DOUBLE))
            .put("group2", ImmutableList.of(BIGINT, BIGINT))
            .put("group3", ImmutableList.of(DOUBLE, DOUBLE))
            .put("group4", ImmutableList.of(createVarcharType(16), createVarcharType(16)))
            .put("group5", ImmutableList.of(createDecimalType(), createDecimalType()))
            .put("group6", ImmutableList.of(createVarcharType(50), createVarcharType(50)))
            .put("group7", ImmutableList.of(DATE, DATE))
            .build();

    @State(Scope.Thread)
    public static class BenchmarkContext
    {
        private ScheduledExecutorService executor;
        private ScheduledExecutorService scheduledExecutor;
        private HttpClient httpClient;
        private ExchangeClientFactory exchangeClientFactory;

        private LoadingCache<String, TestingTaskBuffer> taskBuffers;

        private MergeOperatorFactory operatorFactory;
        private TaskContext testingTaskContext;
        private List<Page> pageTemplate;

        @Param({"group1", "group2", "group3", "group4", "group5", "group6", "group7"})
        String testGroup = "group1";

        @Setup(Level.Trial)
        public void setupTrial()
        {
            executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-merge-omni-operator-%s"));
            scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
            pageTemplate = buildPages();
        }

        @Setup(Level.Invocation)
        public void setupInvocation()
        {
            testingTaskContext = createTaskContext();
            taskBuffers = CacheBuilder.newBuilder().build(CacheLoader.from(TestingTaskBuffer::new));
            httpClient = new TestingHttpClient(new TestingExchangeHttpClientHandler(taskBuffers), executor);
            exchangeClientFactory = new ExchangeClientFactory(new ExchangeClientConfig(), httpClient,
                    executor, new NoOpFailureDetector());
            operatorFactory = createOperatorFactory();
        }

        @TearDown(Level.Trial)
        public void cleanupTrial()
        {
            executor.shutdownNow();
            executor = null;
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;

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

        @TearDown(Level.Invocation)
        public void cleanupInvocation()
        {
            httpClient.close();
            httpClient = null;
            exchangeClientFactory.stop();
            exchangeClientFactory = null;
            taskBuffers.cleanUp();
        }

        protected List<Page> buildPages()
        {
            List<Type> typesArray = INPUT_TYPES.get(testGroup);
            List<Page> pages = new ArrayList<>();
            for (int i = 0; i < TOTAL_PAGES; i++) {
                pages.add(PageBuilderUtil.createSequencePage(typesArray, ROWS_PER_PAGE));
            }
            return pages;
        }

        protected MergeOperatorFactory createOperatorFactory()
        {
            List<Type> totalChannels = INPUT_TYPES.get(testGroup);
            return new MergeOperatorFactory(operatorId.getAndIncrement(),
                    new PlanNodeId("test"), exchangeClientFactory,
                    new OrderingCompiler(), totalChannels, ImmutableList.of(0),
                    ImmutableList.of(0, 1), ImmutableList.of(ASC_NULLS_FIRST, DESC_NULLS_FIRST));
        }

        public TaskContext createTaskContext()
        {
            TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                    .setQueryMaxMemory(new DataSize(2, GIGABYTE)).setTaskStateMachine(new TaskStateMachine(new TaskId("query", 1, 1), executor)).build();
            return taskContext;
        }

        protected List<Page> getPages()
        {
            List<Page> slicedPages = new ArrayList<>();
            for (Page page : pageTemplate) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }
    }

    @Benchmark
    public List<Page> merge(BenchmarkContext context)
    {
        DriverContext driverContext = context.testingTaskContext.addPipelineContext(0, true, true, false)
                .addDriverContext();
        MergeOperator operator = (MergeOperator) context.operatorFactory.createOperator(driverContext);
        operator.addSplit(new Split(ExchangeOperator.REMOTE_CONNECTOR_ID,
                new RemoteSplit(URI.create("http://localhost/" + TASK), "new split test instance id"),
                Lifespan.taskWide()));
        operator.noMoreSplits();
        List<Page> pages = context.getPages();
        LinkedList<Page> outputPages = new LinkedList<>();
        context.taskBuffers.getUnchecked(TASK).addPages(pages, true);

        do {
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
                BlockUtils.freePage(outputPage);
            }
        } while (!operator.isFinished());
        if (pages != null) {
            for (Page page : pages) {
                BlockUtils.freePage(page);
            }
        }
        operator.close();
        driverContext.finished();
        driverContext.getPipelineContext().getTaskContext().getTaskStateMachine().finished();
        return outputPages;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder().verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkMergeOlkOperator.class.getSimpleName() + ".*").build();
        new Runner(options).run();
    }
}
