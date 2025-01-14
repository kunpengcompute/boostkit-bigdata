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

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

public final class UnsafeInMemoryTopNSorter {

    private final MemoryConsumer consumer;
    private final TaskMemoryManager memoryManager;
    private final UnsafePartitionedTopNSorter.TopNSortComparator sortComparator;

    /**
     * Within this buffer, position {@code 2 * i} holds a pointer to the record at index {@code i},
     * while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
     *
     * Only part of the array will be used to store the pointers, the rest part is preserved as temporary buffer for sorting.
     */
    private LongArray array;

    /**
     * The position in the sort buffer where new records can be inserted.
     */
    private int nextEmptyPos = 0;

    // Top n.
    private final int n;
    private final boolean strictTopN;

    // The capacity of array.
    private final int capacity;
    private static final int MIN_ARRAY_CAPACITY = 64;

    public UnsafeInMemoryTopNSorter(
        final int n,
        final boolean strictTopN,
        final MemoryConsumer consumer,
        final TaskMemoryManager memoryManager,
        final UnsafePartitionedTopNSorter.TopNSortComparator sortComparator) {
      this.n = n;
      this.strictTopN = strictTopN;
      this.consumer = consumer;
      this.memoryManager = memoryManager;
      this.sortComparator = sortComparator;
      this.capacity = Math.max(MIN_ARRAY_CAPACITY, Integer.highestOneBit(n) << 1);
      // The size of Long array is equal to twice capacity because each item consists of a prefix and a pointer.
      this.array = consumer.allocateArray(capacity << 1);
    }

    /**
     * Free the memory used by pointer array
     */
    public void freeMemory() {
        if (consumer !=  null) {
            if (array != null) {
                consumer.freeArray(array);
            }
            array = null;
        }
        nextEmptyPos = 0;
    }

    public long getMemoryUsage() {
        if (array == null) {
            return 0L;
        }
        return array.size() * 8;
    }

    public int insert(UnsafeRow row, long prefix) {
        if (nextEmptyPos < n) {
            return insertIntoArray(nextEmptyPos -1, row, prefix);
        } else {
            // reach n candidates
            final int compareResult = nthRecordCompareTo(row, prefix);
            if (compareResult < 0) {
                // skip this record
                return -1;
            }
            else if (compareResult == 0) {
                if (strictTopN) {
                    // For rows that have duplicate values, skip it if this is strict TopN (e.g. RowNumber).
                    return -1;
                }
                // append record
                checkForInsert();
                array.set((nextEmptyPos << 1) + 1, prefix);
                return nextEmptyPos++;
            }
            else {
                checkForInsert();
                // The record at position n -1 should be excluded, so we start comparing with record at position n - 2.
                final int insertPosition = insertIntoArray(n - 2, row, prefix);
                if (strictTopN || insertPosition == n - 1 || hasDistinctTopN()) {
                    nextEmptyPos = n;
                }
                // For other cases, 'nextEmptyPos' will move to the next empty position in 'insertIntoArray()'.
                // e.g. given rank <= 4, and we already have 1, 2, 6, 6, so 'nextEmptyPos' is 4.
                // If the new row is 3, then values in the array will be 1, 2, 3, 6, 6, and 'nextEmptyPos' will be 5.
                return insertPosition;
            }
        }
    }

    public void updateRecordPointer(int position, long pointer) {
        array.set(position << 1, pointer);
    }

    private int insertIntoArray(int position, UnsafeRow row, long prefix) {
        // find insert position
        while (position >= 0 && sortComparator.compare(array.get(position << 1), array.get((position << 1) + 1), row, prefix) > 0) {
            --position;
        }
        final int insertPos = position + 1;

        // move records between 'insertPos' and 'nextEmptyPos' to next positions
        for (int i = nextEmptyPos; i > insertPos; --i) {
            int src = (i - 1) << 1;
            int dst = i << 1;
            array.set(dst, array.get(src));
            array.set(dst + 1, array.get(src + 1));
        }

        // Insert prefix of this row. Note that the address will be inserted by 'updateRecordPointer()'
        // after we get its address from 'taskMemoryManager'
        array.set((insertPos << 1) + 1, prefix);
        ++nextEmptyPos;
        return insertPos;
    }

    private void checkForInsert() {
        if (nextEmptyPos >= capacity) {
            throw new IllegalStateException("No space for new record.\n" +
                    "For RANK expressions with TOP-N filter(e.g. rk <= 100), we maintain a fixed capacity " +
                    "array for TOP-N sorting for each partition, and if there are too many same rankings, " +
                    "the result that needs to be retained will exceed the capacity of the array.\n" +
                    "Please consider using ROW_NUMBER expression or disabling TOP-N sorting by setting " +
                    "saprk.sql.execution.topNPushDownFOrWindow.enabled to false.");
        }
    }

    private int nthRecordCompareTo(UnsafeRow row, long prefix) {
        int nthPos = n - 1;
        return sortComparator.compare(array.get(nthPos << 1), array.get((nthPos << 1) + 1), row, prefix);
    }

    private boolean hasDistinctTopN() {
        int nthPosition = (n - 1) << 1;
        return sortComparator.compare(array.get(nthPosition), array.get(nthPosition + 1), // nth record
                array.get(nthPosition + 2), array.get(nthPosition + 3)) // (n + 1)th record
                != 0; // not eq
    }

    /**
     * This is copied from
     * {@link org.apache.spark.util.collection.unsafe.sort.UnsafeInMemorySorter.SortedIterator}.
     * */
    public final class TopNSortedIterator extends UnsafeSorterIterator implements Cloneable {
        private final int numRecords;
        private int position;
        private int offset;
        private Object baseObject;
        private long baseOffset;
        private long keyPrefix;
        private int recordLength;
        private long currentPageNumber;
        private final TaskContext taskContext = TaskContext.get();

        private TopNSortedIterator(int numRecords, int offset) {
            this.numRecords = numRecords;
            this.position = 0;
            this.offset = offset;
        }

        public TopNSortedIterator clone() {
            TopNSortedIterator iter = new TopNSortedIterator(numRecords, offset);
            iter.position = position;
            iter.baseObject = baseObject;
            iter.baseOffset = baseOffset;
            iter.keyPrefix = keyPrefix;
            iter.recordLength = recordLength;
            iter.currentPageNumber = currentPageNumber;
            return iter;
        }

        @Override
        public int getNumRecords() {
            return numRecords;
        }

        @Override
        public boolean hasNext() {
            return position / 2 < numRecords;
        }

        @Override
        public void loadNext() {
            // Kill the task in case it has been marked as killed. This logic is from
            // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
            // to avoid performance overhead. This check is added here in 'loadNext()' instead of in
            // 'hasNext()' because it's technically possible for the caller to be relying on
            // 'getNumRecords()' instead of 'hasNext()' to know when to stop.
            if (taskContext != null) {
                taskContext.killTaskIfInterrupted();
            }
            // This pointer points to a 4-byte record length, followed by the record's bytes
            final long recordPointer = array.get(offset + position);
            currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            baseObject = memoryManager.getPage(recordPointer);
            // Skip over record length
            baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;
            recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);
            keyPrefix = array.get(offset + position + 1);
            position += 2;
        }

        @Override
        public Object getBaseObject() {
            return baseObject;
        }

        @Override
        public long getBaseOffset() {
            return baseOffset;
        }

        @Override
        public long getCurrentPageNumber() {
            return currentPageNumber;
        }

        @Override
        public int getRecordLength() {
            return recordLength;
        }

        @Override
        public long getKeyPrefix() {
            return keyPrefix;
        }
    }

    /**
     * Return an iterator over record pointers in sorted order. For efficiency, all calls to
     * {@code next()} will return the same mutable object.
     * */
    public UnsafeSorterIterator getSortedIterator() {
        return new TopNSortedIterator(nextEmptyPos, 0);
    }
}