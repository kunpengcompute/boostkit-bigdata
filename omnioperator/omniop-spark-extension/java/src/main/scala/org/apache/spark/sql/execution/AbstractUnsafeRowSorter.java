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

import scala.collection.Iterator;
import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

public abstract class AbstractUnsafeRowSorter
{
    protected final StructType schema;

    /**
     * If positive, forces records to be spilled to disk at the give frequency (measured in numbers of records).
     * This is only intended to be used in tests.
     * */
    protected int testSpillFrequency = 0;

    AbstractUnsafeRowSorter(final StructType schema) {
        this.schema = schema;
    }

    // This flag makes sure the cleanupResource() has been called.
    // After the cleanup work, iterator.next should always return false.
    // Downstream operator triggers the resource cleanup while they found there's no need to keep the iterator anymore.
    // See more detail in SPARK-21492.
    boolean isReleased = false;

    public abstract void insertRow(UnsafeRow row) throws IOException;

    public abstract Iterator<InternalRow> sort() throws IOException;

    public abstract Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException;

    /**
     * @return the peak memory used so far, in bytes.
     * */
    public abstract long getPeakMemoryUsage();

    /**
     * @return the total amount of time spent sorting data (in-memory only).
     * */
    public abstract long getSortTimeNanos();

    public abstract void cleanupResources();

    /**
     * Foreces spills to occur every 'frequency' records. Only for use in tests.
     * */
    @VisibleForTesting
    void setTestSpillFrequency(int frequency) {
        assert frequency > 0 : "Frequency must be positive";
        testSpillFrequency = frequency;
    }

    static final class RowComparator extends RecordComparator {
        private final Ordering<InternalRow> ordering;
        private final UnsafeRow row1;
        private final UnsafeRow row2;

        RowComparator(Ordering<InternalRow> ordering, int numFields) {
            this.row1 = new UnsafeRow(numFields);
            this.row2 = new UnsafeRow(numFields);
            this.ordering = ordering;
        }

        @Override
        public int compare(
                Object baseObj1,
                long baseOff1,
                int baseLen1,
                Object baseObj2,
                long baseOff2,
                int baseLen2) {
            // Note that since ordering doesn't need the total length of the record, we just pass 0 int the row.
            row1.pointTo(baseObj1, baseOff1, 0);
            row2.pointTo(baseObj2, baseOff2, 0);
            return ordering.compare(row1, row2);
        }
    }
}
