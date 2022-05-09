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
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.project.MergePages;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;
import org.glassfish.jersey.internal.guava.Lists;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createBlankVectors;
import static nova.hetu.olk.tool.OperatorUtils.merge;
import static nova.hetu.olk.tool.OperatorUtils.toDataTypes;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromBlocks;

/**
 * The type Omni merge pages.
 *
 * @since 20210630
 */

public class OmniMergePages
{
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

    private OmniMergePages()
    {
    }

    public static WorkProcessor<Page> mergePages(Iterable<? extends Type> types, long minPageSizeInBytes,
                                                 int minRowCount, WorkProcessor<Page> pages, AggregatedMemoryContext memoryContext)
    {
        return mergePages(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, pages, memoryContext);
    }

    public static WorkProcessor<Page> mergePages(Iterable<? extends Type> types, long minPageSizeInBytes,
                                                 int minRowCount, int maxPageSizeInBytes, WorkProcessor<Page> pages, AggregatedMemoryContext memoryContext)
    {
        return pages.transform(new OmniMergePagesTransformation(types, minPageSizeInBytes, minRowCount,
                maxPageSizeInBytes, memoryContext.newLocalMemoryContext(MergePages.class.getSimpleName())));
    }

    public static class OmniMergePagesTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final DataType[] dataTypes;
        private final long minPageSizeInBytes;
        private final int minRowCount;
        private final LocalMemoryContext memoryContext;
        private Page queuedPage;

        List<Page> pages;

        private long currentPageSizeInBytes;

        private long retainedSizeInBytes;

        private int totalPositions;

        private int maxPageSizeInBytes;

        private VecAllocator vecAllocator;

        /**
         * Instantiates a new Omni merge pages.
         *
         * @param types the types
         * @param minPageSizeInBytes the min page size in bytes
         * @param minRowCount the min row count
         * @param maxPageSizeInBytes the max page size in bytes
         * @param memoryContext the memory context
         */
        public OmniMergePagesTransformation(Iterable<? extends Type> types, long minPageSizeInBytes,
                                            int minRowCount, int maxPageSizeInBytes, LocalMemoryContext memoryContext)
        {
            List<Type> inputTypes = Lists.newArrayList(requireNonNull(types, "types is null"));
            this.pages = new ArrayList<>();
            this.dataTypes = toDataTypes(inputTypes);
            this.maxPageSizeInBytes = maxPageSizeInBytes;
            checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
            checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
            checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
            checkArgument(maxPageSizeInBytes >= minPageSizeInBytes,
                    "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
            checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %d",
                    MAX_MIN_PAGE_SIZE);
            this.minPageSizeInBytes = minPageSizeInBytes;
            this.minRowCount = minRowCount;
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        }

        /**
         * Gets retained size in bytes.
         *
         * @return the retained size in bytes
         */
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        @Override
        public WorkProcessor.TransformationState<Page> process(Page inputPage)
        {
            if (queuedPage != null) {
                Page output = queuedPage;
                queuedPage = null;
                memoryContext.setBytes(getRetainedSizeInBytes());
                return ofResult(output);
            }

            boolean inputFinished = inputPage == null;
            if (inputFinished) {
                if (pages.isEmpty()) {
                    memoryContext.close();
                    return finished();
                }

                return ofResult(flush(), false);
            }

            if (inputPage.getPositionCount() >= minRowCount || inputPage.getSizeInBytes() >= minPageSizeInBytes) {
                if (pages.isEmpty()) {
                    return ofResult(inputPage);
                }

                Page output = flush();
                // inputPage is preserved until next process(...) call
                queuedPage = inputPage;
                memoryContext.setBytes(getRetainedSizeInBytes() + inputPage.getRetainedSizeInBytes());
                return ofResult(output, false);
            }

            appendPage(inputPage);

            if (isFull()) {
                return ofResult(flush());
            }

            memoryContext.setBytes(getRetainedSizeInBytes());
            return needsMoreData();
        }

        /**
         * This method takes in one page at a time and buffers it in a list so that the
         * final merged page can be created.
         *
         * @param page A Page to be merged.
         */
        public void appendPage(Page page)
        {
            // VecAllocator is only created once
            if (this.vecAllocator == null) {
                this.vecAllocator = getVecAllocatorFromBlocks(page.getBlocks());
            }
            pages.add(page);
            totalPositions += page.getPositionCount();
            currentPageSizeInBytes = currentPageSizeInBytes + page.getSizeInBytes();
            retainedSizeInBytes = retainedSizeInBytes + page.getRetainedSizeInBytes();
        }

        /**
         * Is full boolean.
         *
         * @return the boolean
         */
        public boolean isFull()
        {
            return totalPositions == Integer.MAX_VALUE || currentPageSizeInBytes >= maxPageSizeInBytes;
        }

        /**
         * This method is invoked to retrieve the final merged page.
         *
         * @return Page output
         */
        public Page flush()
        {
            VecBatch mergeResult = new VecBatch(createBlankVectors(vecAllocator, dataTypes, totalPositions),
                    totalPositions);
            merge(mergeResult, pages, vecAllocator);
            Page finalPage = new VecBatchToPageIterator(ImmutableList.of(mergeResult).iterator()).next();
            currentPageSizeInBytes = 0;
            retainedSizeInBytes = 0;
            totalPositions = 0;
            pages.clear();
            return finalPage;
        }
    }
}
