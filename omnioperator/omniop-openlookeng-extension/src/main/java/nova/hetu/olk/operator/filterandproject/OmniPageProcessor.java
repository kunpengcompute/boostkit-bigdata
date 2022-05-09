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

package nova.hetu.olk.operator.filterandproject;

import com.google.common.collect.ImmutableList;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.SelectedPositions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.sql.gen.ExpressionProfiler;
import nova.hetu.olk.OmniLocalExecutionPlanner.OmniLocalExecutionPlanContext;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.prestosql.operator.WorkProcessor.ProcessState.finished;
import static io.prestosql.operator.WorkProcessor.ProcessState.ofResult;
import static io.prestosql.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_NATIVE_ERROR;

/**
 * The type Omni page processor.
 *
 * @since 20210630
 */
@NotThreadSafe
public class OmniPageProcessor
        extends PageProcessor
{
    private final OmniProjection projection;

    private final VecAllocator vecAllocator;

    private final OmniLocalExecutionPlanContext context;

    private Optional<OmniPageFilter.OmniPageFilterOperator> omniPageFilterOperator = Optional.empty();
    private Optional<OmniOperator> omniProjectionOperator = Optional.empty();

    /**
     * Instantiates a new Omni page processor.
     *
     * @param filter the filter
     * @param proj the proj
     * @param initialBatchSize the initial batch size
     * @param expressionProfiler the expression profiler
     */
    public OmniPageProcessor(VecAllocator vecAllocator, Optional<PageFilter> filter, OmniProjection proj,
                             OptionalInt initialBatchSize, ExpressionProfiler expressionProfiler,
                             OmniLocalExecutionPlanContext context)
    {
        super(filter, Collections.emptyList(), initialBatchSize, expressionProfiler);
        this.vecAllocator = vecAllocator;
        this.context = context;
        this.projection = requireNonNull(proj, "projection is null");

        if (filter.isPresent()) {
            PageFilter pageFilter = filter.get();
            this.omniPageFilterOperator = Optional.of(((OmniPageFilter) pageFilter).getOperator(vecAllocator));
            if (context != null) {
                context.onTaskFinished(taskFinished -> this.omniPageFilterOperator.get().close());
            }
        }
        else {
            this.omniProjectionOperator = Optional.of(projection.getFactory().createOperator(vecAllocator));
            if (context != null) {
                context.onTaskFinished(taskFinished -> this.omniProjectionOperator.get().close());
            }
        }
    }

    public OmniProjection getProjection()
    {
        return this.projection;
    }

    private Page preloadNeedFilterLazyBlock(Page page)
    {
        if (!omniPageFilterOperator.isPresent()) {
            return page;
        }
        List<Integer> inputChannels = omniPageFilterOperator.get().getInputChannels().getInputChannels();
        if (inputChannels.isEmpty()) {
            return page;
        }

        int blockSize = page.getBlocks().length;
        Block[] blocks = new Block[blockSize];
        for (int i = 0; i < blockSize; i++) {
            Block block = page.getBlock(i);
            if (block instanceof LazyBlock && inputChannels.contains(i)) {
                blocks[i] = block.getLoadedBlock();
            }
            else {
                blocks[i] = block;
            }
        }
        return new Page(blocks);
    }

    @Override
    public WorkProcessor<Page> createWorkProcessor(ConnectorSession session, DriverYieldSignal yieldSignal,
                                                   LocalMemoryContext memoryContext, Page page)
    {
        if (page.getPositionCount() == 0) {
            BlockUtils.freePage(page);
            return WorkProcessor.of();
        }

        Page preloadPage = preloadNeedFilterLazyBlock(page);
        VecBatch inputVecBatch = buildVecBatch(vecAllocator, preloadPage, this);
        if (omniPageFilterOperator.isPresent()) {
            VecBatch filteredVecBatch = omniPageFilterOperator.get().filterAndProject(inputVecBatch);
            if (filteredVecBatch == null) {
                return WorkProcessor.of();
            }
            Iterator<Page> result = new VecBatchToPageIterator(ImmutableList.of(filteredVecBatch).iterator());
            if (result.hasNext()) {
                return WorkProcessor.of(result.next());
            }
            else {
                return WorkProcessor.of();
            }
        }

        return WorkProcessor.create(new OmniProjectSelectedPositions(vecAllocator, session, yieldSignal, memoryContext,
                inputVecBatch, positionsRange(0, inputVecBatch.getRowCount()), omniProjectionOperator.get()));
    }

    private class OmniProjectSelectedPositions
            implements WorkProcessor.Process<Page>
    {
        private final VecAllocator vecAllocator;

        private final ConnectorSession session;

        private final DriverYieldSignal yieldSignal;

        private final LocalMemoryContext memoryContext;

        private final VecBatch vecBatch;

        private final SelectedPositions selectedPositions;

        private boolean isFinished;
        private final OmniOperator omniProjectionOperator;

        /**
         * Instantiates a new Omni project selected positions.
         *
         * @param vecAllocator vector allocator
         * @param session the session
         * @param yieldSignal the yield signal
         * @param memoryContext the memory context
         * @param vecBatch the page
         * @param selectedPositions the selected positions
         */
        public OmniProjectSelectedPositions(VecAllocator vecAllocator, ConnectorSession session,
                                            DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, VecBatch vecBatch,
                                            SelectedPositions selectedPositions, OmniOperator omniProjectionOperator)
        {
            this.vecAllocator = vecAllocator;
            this.omniProjectionOperator = omniProjectionOperator;
            this.session = session;
            this.yieldSignal = yieldSignal;
            this.memoryContext = memoryContext;
            this.vecBatch = vecBatch;
            this.selectedPositions = selectedPositions;
            this.isFinished = false;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (isFinished) {
                return finished();
            }

            omniProjectionOperator.addInput(vecBatch);
            Iterator<Page> result = new VecBatchToPageIterator(omniProjectionOperator.getOutput());
            if (!result.hasNext()) {
                throw new OmniRuntimeException(OMNI_NATIVE_ERROR, "Filter returns empty result");
            }
            Page projectedPage = result.next();
            isFinished = true;
            return ofResult(projectedPage);
        }
    }
}
