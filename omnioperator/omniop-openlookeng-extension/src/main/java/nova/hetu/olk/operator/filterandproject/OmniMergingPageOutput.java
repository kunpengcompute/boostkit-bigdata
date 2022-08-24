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
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.createBlankVectors;
import static nova.hetu.olk.tool.OperatorUtils.merge;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromBlocks;

/**
 * This class is intended to be used right after the PageProcessor to ensure
 * that the size of the pages returned by FilterAndProject and
 * ScanFilterAndProject is big enough so it does not introduce considerable
 * synchronization overhead.
 * <p>
 * As long as the input page contains more than
 * {@link OmniMergingPageOutput#minRowCount} rows or is bigger than
 * {@link OmniMergingPageOutput#minPageSizeInBytes} it is returned as is without
 * additional memory copy.
 * <p>
 * The page data that has been buffered so far before receiving a "big" page is
 * being flushed before transferring a "big" page.
 * <p>
 * Although it is still possible that the {@link OmniMergingPageOutput} may
 * return a tiny page, this situation is considered to be rare due to the
 * assumption that filter selectivity may not vary a lot based on the particular
 * input page.
 * <p>
 * Considering the CPU time required to process(filter, project) a full (~1MB)
 * page returned by a connector, the CPU cost of memory copying (< 50kb, < 1024
 * rows) is supposed to be negligible.
 *
 * @since 20210930
 */
public class OmniMergingPageOutput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OmniMergingPageOutput.class).instanceSize();
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

    private final DataType[] dataTypes;
    private final List<Page> bufferedPages;
    private final Queue<Page> outputQueue = new LinkedList<>();

    private final long maxPageSizeInBytes;
    private final long minPageSizeInBytes;
    private final int minRowCount;

    @Nullable
    private Iterator<Optional<Page>> currentInput;
    private boolean finishing;
    private int totalPositions;
    private long currentPageSizeInBytes;
    private long retainedSizeInBytes;
    private VecAllocator vecAllocator;

    public OmniMergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount)
    {
        this(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    public OmniMergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount,
                                 VecAllocator vecAllocator)
    {
        this(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        this.vecAllocator = vecAllocator;
    }

    public OmniMergingPageOutput(Iterable<? extends Type> types, long minPageSizeInBytes, int minRowCount,
                                 int maxPageSizeInBytes)
    {
        List<Type> blockTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.dataTypes = blockTypes.stream().map(OperatorUtils::toDataType).toArray(DataType[]::new);
        checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
        checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
        checkArgument(maxPageSizeInBytes >= minPageSizeInBytes,
                "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
        checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %s",
                MAX_MIN_PAGE_SIZE);
        this.maxPageSizeInBytes = maxPageSizeInBytes;
        this.minPageSizeInBytes = minPageSizeInBytes;
        this.minRowCount = minRowCount;
        this.bufferedPages = new LinkedList<>();
    }

    /**
     * Check if need more input pages
     *
     * @return if need more input pages
     */
    public boolean needsInput()
    {
        return currentInput == null && !finishing && outputQueue.isEmpty();
    }

    /**
     * Add more pages to be merged
     *
     * @param input pages to be merged
     */
    public void addInput(Iterator<Optional<Page>> input)
    {
        requireNonNull(input, "input is null");
        checkState(!finishing, "output is in finishing state");
        checkState(currentInput == null, "currentInput is present");
        currentInput = input;
    }

    /**
     * Get a merged page
     *
     * @return A merged page
     */
    @Nullable
    public Page getOutput()
    {
        if (!outputQueue.isEmpty()) {
            return outputQueue.poll();
        }

        while (currentInput != null) {
            if (!currentInput.hasNext()) {
                currentInput = null;
                break;
            }

            if (!outputQueue.isEmpty()) {
                break;
            }

            Optional<Page> next = currentInput.next();
            if (next.isPresent()) {
                process(next.get());
            }
            else {
                break;
            }
        }

        if (currentInput == null && finishing) {
            flush();
        }

        return outputQueue.poll();
    }

    /**
     * Start finishing
     */
    public void finish()
    {
        finishing = true;
    }

    /**
     * Check if finished
     *
     * @return if finished
     */
    public boolean isFinished()
    {
        return finishing && currentInput == null && outputQueue.isEmpty() && bufferedPages.isEmpty();
    }

    private void process(Page page)
    {
        requireNonNull(page, "page is null");

        // avoid memory copying for pages that are big enough
        if (page.getPositionCount() >= minRowCount || page.getSizeInBytes() >= minPageSizeInBytes) {
            flush();
            outputQueue.add(page);
            return;
        }

        buffer(page);
    }

    private void buffer(Page page)
    {
        // VecAllocator is only created once
        if (this.vecAllocator == null) {
            this.vecAllocator = getVecAllocatorFromBlocks(page.getBlocks());
        }
        totalPositions += page.getPositionCount();
        bufferedPages.add(page);
        currentPageSizeInBytes = currentPageSizeInBytes + page.getSizeInBytes();
        retainedSizeInBytes = retainedSizeInBytes + page.getRetainedSizeInBytes();

        if (isFull()) {
            flush();
        }
    }

    private void flush()
    {
        if (bufferedPages.isEmpty()) {
            return;
        }

        VecBatch resultVecBatch = new VecBatch(createBlankVectors(vecAllocator, dataTypes, totalPositions),
                totalPositions);
        merge(resultVecBatch, bufferedPages, vecAllocator);
        outputQueue.add(new VecBatchToPageIterator(ImmutableList.of(resultVecBatch).iterator()).next());

        // reset buffers
        bufferedPages.clear();
        currentPageSizeInBytes = 0;
        retainedSizeInBytes = 0;
        totalPositions = 0;
    }

    /**
     * Get retained size in bytes
     *
     * @return retained size in bytes
     */
    public long getRetainedSizeInBytes()
    {
        long retainedSize = INSTANCE_SIZE + this.retainedSizeInBytes;
        for (Page page : outputQueue) {
            retainedSize += page.getRetainedSizeInBytes();
        }
        return retainedSize;
    }

    private boolean isFull()
    {
        return totalPositions == Integer.MAX_VALUE || currentPageSizeInBytes >= maxPageSizeInBytes;
    }

    public void close()
    {
        // free input page due to it may not be handled
        if (currentInput != null) {
            while (currentInput.hasNext()) {
                Optional<Page> page = currentInput.next();
                if (page.isPresent()) {
                    BlockUtils.freePage(page.get());
                }
            }
        }

        // free page in buffer
        if (!bufferedPages.isEmpty()) {
            for (Page page : bufferedPages) {
                BlockUtils.freePage(page);
            }
        }

        // free page in output queue
        while (!outputQueue.isEmpty()) {
            Page page = outputQueue.poll();
            if (page != null) {
                BlockUtils.freePage(page);
            }
        }
    }
}
