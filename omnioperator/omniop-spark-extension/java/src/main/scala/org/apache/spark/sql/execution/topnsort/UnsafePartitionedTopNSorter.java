/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.topnsort;

import java.util.*;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.*;

/**
 * Partitioned top n sorter based on {@link org.apache.spark.sql.execution.topnsort.UnsafeInMemoryTopNSorter}.
 * The implementation mostly refers to {@link UnsafeExternalSorter}.
 * */
public final class UnsafePartitionedTopNSorter extends MemoryConsumer {
    private final TaskMemoryManager taskMemoryManager;
    private TopNSortComparator sortComparator;

    /**
     * Memory pages that hold the records being sorted. The pages in this list are freed when
     * spilling, although in principle we could recycle these pages across spills (on the other hand,
     * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager itself).
     * */
    private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();
    private final Map<UnsafeRow, UnsafeInMemoryTopNSorter> partToSorters = new LinkedHashMap<>();

    private final int n;
    private final boolean strictTopN;
    private MemoryBlock currentPage = null;
    private long pageCursor = -1;
    private long peakMemoryUsedBytes = 0;

    public static UnsafePartitionedTopNSorter create(
            int n,
            boolean strictTopN,
            TaskMemoryManager taskMemoryManager,
            TaskContext taskContext,
            Supplier<RecordComparator> recordComparatorSupplier,
            PrefixComparator prefixComparator,
            long pageSizeBytes,
            boolean canSortFullyWithPrefix) {
        assert n > 0 : "Top n must be positive";
        assert recordComparatorSupplier != null;
        return new UnsafePartitionedTopNSorter(n, strictTopN, taskMemoryManager, taskContext,
                recordComparatorSupplier, prefixComparator, pageSizeBytes, canSortFullyWithPrefix);
    }

    private UnsafePartitionedTopNSorter(
            int n,
            boolean strictTopN,
            TaskMemoryManager taskMemoryManager,
            TaskContext taskContext,
            Supplier<RecordComparator> recordComparatorSupplier,
            PrefixComparator prefixComparator,
            long pageSizeBytes,
            boolean canSortFullyWithPrefix) {
        super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
        this.n = n;
        this.strictTopN = strictTopN;
        this.taskMemoryManager = taskMemoryManager;
        this.sortComparator = new TopNSortComparator(recordComparatorSupplier.get(),
                prefixComparator, taskMemoryManager, canSortFullyWithPrefix);

        // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
        // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
        // does not fully consume the sorter's output (e.g. sort followed by limit).
        taskContext.addTaskCompletionListener(context -> {
            cleanupResources();
        });
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) {
        throw new UnsupportedOperationException("Spill is unsupported operation in topN in-memory sorter");
    }

    /**
     * Return the total memory usage of this sorter, including the data pages and the sorter's pointer array.
     * */
    private long getMemoryUsage() {
        long totalPageSize = 0;
        for (MemoryBlock page : allocatedPages) {
            totalPageSize += page.size();
        }
        for (UnsafeInMemoryTopNSorter sorter : partToSorters.values()) {
            totalPageSize += sorter.getMemoryUsage();
        }
        return totalPageSize;
    }

    private void updatePeakMemoryUsed() {
        long mem = getMemoryUsage();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     * */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    @VisibleForTesting
    public int getNumberOfAllocatedPages() {
        return allocatedPages.size();
    }

    /**
     * Free this sorter's data pages.
     *
     * @return the number of bytes freed.
     * */
    private long freeMemory() {
        updatePeakMemoryUsed();
        long memoryFreed = 0;
        for (MemoryBlock block : allocatedPages) {
            memoryFreed += block.size();
            freePage(block);
        }
        allocatedPages.clear();
        currentPage = null;
        pageCursor = 0;
        for (UnsafeInMemoryTopNSorter sorter: partToSorters.values()) {
            memoryFreed += sorter.getMemoryUsage();
            sorter.freeMemory();
        }
        partToSorters.clear();
        sortComparator = null;
        return  memoryFreed;
    }

    /**
     * Frees this sorter's in-memory data structures and cleans up its spill files.
     * */
    public void cleanupResources() {
        synchronized (this) {
            freeMemory();
        }
    }

    /**
     * Allocates an additional page in order to insert an additional record. This will request
     * additional memory from the memory manager and spill if the requested memory can not be obtained.
     *
     * @param required the required space in the data page, in bytes, including space for storing the record size
     * */
    private void acquireNewPageIfNecessary(int required) {
        if (currentPage == null ||
            pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
            currentPage = allocatePage(required);
            pageCursor = currentPage.getBaseOffset();
            allocatedPages.add(currentPage);
        }
    }

    public void insertRow(UnsafeRow partKey, UnsafeRow row, long prefix) {
        UnsafeInMemoryTopNSorter sorter =
                partToSorters.computeIfAbsent(
                        partKey,
                        k -> new UnsafeInMemoryTopNSorter(n, strictTopN, this, taskMemoryManager, sortComparator)
                );
        final int position = sorter.insert(row, prefix);
        if (position >= 0) {
            final int uaoSize = UnsafeAlignedOffset.getUaoSize();
            // Need 4 or 8 bytes to store the record length.
            final int length = row.getSizeInBytes();
            final int required = length + uaoSize;
            acquireNewPageIfNecessary(required);

            final Object base = currentPage.getBaseObject();
            final long recordAddress =
                    taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
            UnsafeAlignedOffset.putSize(base, pageCursor, length);
            pageCursor += uaoSize;
            Platform.copyMemory(row.getBaseObject(), row.getBaseOffset(), base, pageCursor, length);
            pageCursor += length;

            sorter.updateRecordPointer(position, recordAddress);
        }
    }

    public Map<UnsafeRow, UnsafeInMemoryTopNSorter> getPartKeyToSorter() {
        return partToSorters;
    }

    static final class TopNSortComparator {
        private final RecordComparator recordComparator;
        private final PrefixComparator prefixComparator;
        private final TaskMemoryManager memoryManager;
        private final boolean needCompareFully;

        TopNSortComparator(
                RecordComparator recordComparator,
                PrefixComparator prefixComparator,
                TaskMemoryManager memoryManager,
                boolean canSortFullyWithPrefix) {
            this.recordComparator = recordComparator;
            this.prefixComparator = prefixComparator;
            this.memoryManager = memoryManager;
            this.needCompareFully = !canSortFullyWithPrefix;
        }

        public int compare(long pointer1, long prefix1, long pointer2, long prefix2) {
            final int prefixComparisonResult = prefixComparator.compare(prefix1, prefix2);
            if (needCompareFully && prefixComparisonResult == 0) {
                final int uaoSize = UnsafeAlignedOffset.getUaoSize();
                final Object baseObject1 = memoryManager.getPage(pointer1);
                final long baseOffset1 = memoryManager.getOffsetInPage(pointer1) + uaoSize;
                final int baseLength1 = UnsafeAlignedOffset.getSize(baseObject1, baseOffset1 - uaoSize);
                final Object baseObject2 = memoryManager.getPage(pointer2);
                final long baseOffset2 = memoryManager.getOffsetInPage(pointer2) + uaoSize;
                final int baseLength2 = UnsafeAlignedOffset.getSize(baseObject2, baseOffset2 - uaoSize);
                return recordComparator.compare(baseObject1, baseOffset1, baseLength1, baseObject2,
                        baseOffset2, baseLength2);
            } else {
                return prefixComparisonResult;
            }
        }

        public int compare(long pointer, long prefix1, UnsafeRow row, long prefix2) {
            final int prefixComparisonResult = prefixComparator.compare(prefix1, prefix2);
            if (needCompareFully && prefixComparisonResult == 0) {
                final int uaoSize = UnsafeAlignedOffset.getUaoSize();
                final Object baseObject1 = memoryManager.getPage(pointer);
                final long baseOffset1 = memoryManager.getOffsetInPage(pointer) + uaoSize;
                final int baseLength1 = UnsafeAlignedOffset.getSize(baseObject1, baseOffset1 - uaoSize);
                final Object baseObject2 = row.getBaseObject();
                final long baseOffset2 = row.getBaseOffset();
                final int baseLength2 = row.getSizeInBytes();
                return recordComparator.compare(baseObject1, baseOffset1, baseLength1, baseObject2,
                        baseOffset2, baseLength2);
            } else {
                return prefixComparisonResult;
            }
        }
    }
}
