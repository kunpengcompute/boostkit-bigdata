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

import io.airlift.units.DataSize;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStateMachine;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Level.Trial;

public abstract class AbstractOperatorBenchmarkContext
{
    private static final boolean INCLUDE_CREATE_OPERATOR = true;

    private static final AtomicInteger queryIdGenerator = new AtomicInteger();
    protected ExecutorService executor;

    protected ScheduledExecutorService scheduledExecutor;

    protected OperatorFactory operatorFactory;

    private List<Page> pageTemplate;

    private InvocationContext invocationContext;

    public static class InvocationContext
    {
        private LinkedList<Page> pages;
        private DriverContext driverContext;
        private LinkedList<Page> output;
        private Operator operator;
    }

    public void setup()
    {
        this.setupTrial();
        this.setupIteration();
    }

    public void cleanup()
    {
        this.cleanupIteration();
        this.cleanupTrial();
    }

    protected void beforeSetupTrial()
    {
    }

    @Setup(Trial)
    public void setupTrial()
    {
        beforeSetupTrial();
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        operatorFactory = createOperatorFactory();
        pageTemplate = buildPages();
        afterSetupTrial();
    }

    protected void afterSetupTrial()
    {
    }

    protected void beforeSetupIteration()
    {
    }

    @Setup(Invocation)
    public void setupIteration()
    {
        beforeSetupIteration();
        InvocationContext invocationContext = new InvocationContext();
        invocationContext.driverContext = createTaskContext()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        invocationContext.pages = new LinkedList<>(forkPages(this.pageTemplate));
        if (!INCLUDE_CREATE_OPERATOR) {
            invocationContext.operator = operatorFactory.createOperator(invocationContext.driverContext);
        }
        this.invocationContext = invocationContext;
        afterSetupIteration();
    }

    protected void afterSetupIteration()
    {
    }

    protected abstract List<Page> buildPages();

    protected abstract List<Page> forkPages(List<Page> pages);

    protected abstract OperatorFactory createOperatorFactory();

    protected TaskContext createTaskContext()
    {
        return createTaskContextBySizeInGigaByte(2);
    }

    protected TaskContext createTaskContextBySizeInGigaByte(int gigaByte)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(new DataSize(gigaByte, GIGABYTE)).setTaskStateMachine(new TaskStateMachine(new TaskId("query", 1, queryIdGenerator.incrementAndGet()), executor)).build();
    }

    protected void beforeCleanupIteration()
    {
    }

    @TearDown(Invocation)
    public void cleanupIteration()
    {
        beforeCleanupIteration();
        closeOperator();
        if (invocationContext.output != null) {
            for (Page page : invocationContext.output) {
                BlockUtils.freePage(page);
            }
        }
        if (getRemainInputPages() != null) {
            for (Page page : getRemainInputPages()) {
                BlockUtils.freePage(page);
            }
        }
        invocationContext.driverContext.finished();
        invocationContext.driverContext.getPipelineContext().getTaskContext().getTaskStateMachine().finished();
        invocationContext = null;
        afterCleanupIteration();
    }

    protected void afterCleanupIteration()
    {
    }

    protected void beforeCleanupTrial()
    {
    }

    @TearDown(Trial)
    public void cleanupTrial()
    {
        beforeCleanupTrial();
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        afterCleanupTrial();
    }

    protected void afterCleanupTrial()
    {
    }

    public final Operator createOperator()
    {
        if (INCLUDE_CREATE_OPERATOR) {
            invocationContext.operator = operatorFactory.createOperator(invocationContext.driverContext);
        }
        return invocationContext.operator;
    }

    public final void closeOperator()
    {
        try {
            invocationContext.operator.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public final List<Page> getPages()
    {
        return pageTemplate;
    }

    public final LinkedList<Page> getRemainInputPages()
    {
        return invocationContext.pages;
    }

    public List<Page> setOutput(LinkedList<Page> output)
    {
        this.invocationContext.output = output;
        return output;
    }

    public final List<Page> doDefaultBenchMark()
    {
        Operator operator = createOperator();
        Iterator<Page> input = getRemainInputPages().iterator();
        LinkedList<Page> outputPages = new LinkedList<>();

        while (input.hasNext()) {
            if (!operator.needsInput() && !operator.isFinished()) {
                Page output = operator.getOutput();
                if (output != null) {
                    outputPages.add(output);
                }
            }
            Page next = input.next();
            if (operator.needsInput()) {
                operator.addInput(next);
                input.remove();
            }
        }

        operator.finish();

        do {
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
        } while (!operator.isFinished());

        return setOutput(outputPages);
    }

    abstract static class AbstractOmniOperatorBenchmarkContext
            extends AbstractOperatorBenchmarkContext
    {
        private VecAllocator taskLevelAllocator;

        @Override
        protected TaskContext createTaskContext()
        {
            TaskContext taskContext = super.createTaskContext();
            taskLevelAllocator = VecAllocatorHelper.createTaskLevelAllocator(taskContext);
            return taskContext;
        }

        @Override
        protected List<Page> forkPages(List<Page> pages)
        {
            List<Page> slicedPages = new ArrayList<>(pages.size());
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return transferToOffHeapPages(taskLevelAllocator, slicedPages);
        }
    }

    abstract static class AbstractOlkOperatorBenchmarkContext
            extends AbstractOperatorBenchmarkContext
    {
        @Override
        protected List<Page> forkPages(List<Page> pages)
        {
            List<Page> slicedPages = new ArrayList<>(pages.size());
            for (Page page : pages) {
                slicedPages.add(page.getRegion(0, page.getPositionCount()));
            }
            return slicedPages;
        }
    }
}
