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

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import scala.collection.Iterator;
import scala.math.Ordering;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.topnsort.UnsafeInMemoryTopNSorter;
import org.apache.spark.sql.execution.topnsort.UnsafePartitionedTopNSorter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

public final class UnsafeTopNRowSorter extends AbstractUnsafeRowSorter {

    private final UnsafePartitionedTopNSorter partitionedTopNSorter;

    // partition key
    private final UnsafeProjection partitionSpecProjection;

    // order(rank) key
    private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;

    private long totalSortTimeNanos = 0L;
    private final long timeNanosBeforeInsertRow;

    public static UnsafeTopNRowSorter create(
            int n,
            boolean strictTopN,
            StructType schema,
            UnsafeProjection partitionSpecProjection,
            Ordering<InternalRow> orderingOfRankKey,
            PrefixComparator prefixComparator,
            UnsafeExternalRowSorter.PrefixComputer prefixComputer,
            long pageSizeBytes,
            boolean canSortFullyWithPrefix) {
        Supplier<RecordComparator> recordComparatorSupplier =
                () -> new RowComparator(orderingOfRankKey, schema.length());
        return new UnsafeTopNRowSorter(
                n, strictTopN, schema, partitionSpecProjection, recordComparatorSupplier,
                prefixComparator, prefixComputer, pageSizeBytes, canSortFullyWithPrefix);
    }

    private UnsafeTopNRowSorter(
            int n,
            boolean strictTopN,
            StructType schema,
            UnsafeProjection partitionSpecProjection,
            Supplier<RecordComparator> recordComparatorSupplier,
            PrefixComparator prefixComparator,
            UnsafeExternalRowSorter.PrefixComputer prefixComputer,
            long pageSizeBytes,
            boolean canSortFullyWithPrefix) {
        super(schema);
        this.prefixComputer = prefixComputer;
        final TaskContext taskContext = TaskContext.get();
        this.partitionSpecProjection = partitionSpecProjection;
        this.partitionedTopNSorter = UnsafePartitionedTopNSorter.create(
                n,
                strictTopN,
                taskContext.taskMemoryManager(),
                taskContext,
                recordComparatorSupplier,
                prefixComparator,
                pageSizeBytes,
                canSortFullyWithPrefix);
        timeNanosBeforeInsertRow = System.nanoTime();
    }

    @Override
    public void insertRow(UnsafeRow row) throws IOException {
        final UnsafeExternalRowSorter.PrefixComputer.Prefix prefix = prefixComputer.computePrefix(row);
        UnsafeRow partKey = partitionSpecProjection.apply(row);
        partitionedTopNSorter.insertRow(partKey, row, prefix.value);
    }

    /**
     * Return the peak memory used so far, in bytes.
     * */
    @Override
    public long getPeakMemoryUsage() {
        return partitionedTopNSorter.getPeakMemoryUsedBytes();
    }

    /**
     * @return the total amount of time spent sorting data (in-memory only).
     * */
    @Override
    public long getSortTimeNanos() {
        return totalSortTimeNanos;
    }

    @Override
    public Iterator<InternalRow> sort() throws IOException
    {
        try {
            Map<UnsafeRow, UnsafeInMemoryTopNSorter> partKeyToSorter =
                    partitionedTopNSorter.getPartKeyToSorter();
            if (partKeyToSorter.isEmpty()) {
                // Since we won't ever call next() on an empty iterator, we need to clean up resources
                // here in order to prevent memory leaks.
                cleanupResources();
                return emptySortedIterator();
            }

            Queue<RowIterator> sortedIteratorsForPartitions = new LinkedList<>();
            for (Map.Entry<UnsafeRow, UnsafeInMemoryTopNSorter> entry : partKeyToSorter.entrySet()) {
                final UnsafeInMemoryTopNSorter topNSorter = entry.getValue();
                final UnsafeSorterIterator unsafeSorterIterator = topNSorter.getSortedIterator();

                sortedIteratorsForPartitions.add(new RowIterator()
                {
                    private final int numFields = schema.length();
                    private UnsafeRow row = new UnsafeRow(numFields);

                    @Override
                    public boolean advanceNext()
                    {
                        try {
                            if (!isReleased && unsafeSorterIterator.hasNext()) {
                                unsafeSorterIterator.loadNext();
                                row.pointTo(
                                        unsafeSorterIterator.getBaseObject(),
                                        unsafeSorterIterator.getBaseOffset(),
                                        unsafeSorterIterator.getRecordLength());
                                // Here is the initial buf ifx in SPARK-9364: the bug fix of use-after-free bug
                                // when returning the last row from an iterator. For example, in
                                // [[GroupedIterator]], we still use the last row after traversing the iterator
                                // in 'fetchNextGroupIterator'
                                if (!unsafeSorterIterator.hasNext()) {
                                    row = row.copy(); // so that we don't have dangling pointers to freed page
                                    topNSorter.freeMemory();
                                }
                                return true;
                            }
                            else {
                                row = null; // so that we don't keep reference to the base object
                                return false;
                            }
                        } catch (IOException e) {
                            topNSorter.freeMemory();
                            // Scala iterators don't declare any checked exceptions, so we need to use this hack
                            // to re-throw the exception.
                            Platform.throwException(e);
                        }
                        throw new RuntimeException("Exception should have been re-thrown in next()");
                    }

                    @Override
                    public UnsafeRow getRow()
                    {
                        return row;
                    }
                });
            }

            // Update total sort time.
            if (totalSortTimeNanos == 0L) {
                totalSortTimeNanos = System.nanoTime() - timeNanosBeforeInsertRow;
            }
            final ChainedIterator chainedIterator = new ChainedIterator(sortedIteratorsForPartitions);
            return chainedIterator.toScala();
        } catch (Exception e) {
            cleanupResources();
            throw e;
        }
    }

    private Iterator<InternalRow> emptySortedIterator() {
        return new RowIterator() {
            @Override
            public boolean advanceNext() {
                return false;
            }

            @Override
            public UnsafeRow getRow() {
                return null;
            }
        }.toScala();
    }

    /**
     * Chain multiple UnsafeSorterIterators from PartSorterMap as single one.
     * */
    private static final class ChainedIterator extends RowIterator {
        private final Queue<RowIterator> iterators;
        private RowIterator current;
        private UnsafeRow row;

        ChainedIterator(Queue<RowIterator> iterators) {
            assert iterators.size() > 0;
            this.iterators = iterators;
            this.current = iterators.remove();
        }

        @Override
        public boolean advanceNext() {
            boolean result = this.current.advanceNext();
            while(!result && !this.iterators.isEmpty()) {
                this.current = iterators.remove();
                result = this.current.advanceNext();
            }
            if (!result) {
                this.row = null;
            } else {
                this.row = (UnsafeRow) this.current.getRow();
            }
            return result;
        }

        @Override
        public UnsafeRow getRow() {
            return row;
        }
    }

    @Override
    public Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
        while (inputIterator.hasNext()) {
            insertRow(inputIterator.next());
        }
        return sort();
    }

    @Override
    public void cleanupResources() {
        isReleased = true;
        partitionedTopNSorter.cleanupResources();
    }
}
