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

package org.apache.spark.util.collection.unsafe.sort;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 */
public final class UnsafeInMemoryRadixSorter {

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;

  /**
   * If non-null, specifies the radix sort parameters and that radix sort will be used.
   */
  @Nullable
  private final List<PrefixComparators.RadixSortSupport> radixSortSupports;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   *
   * Only part of the array will be used to store the pointers, the rest part is preserved as
   * temporary buffer for sorting.
   */
  private LongArray array;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0;

  /**
   * If sorting with radix sort, specifies the starting position in the sort buffer where records
   * with non-null prefixes are kept. Positions [0..nullBoundaryPos) will contain null-prefixed
   * records, and positions [nullBoundaryPos..pos) non-null prefixed records. This lets us avoid
   * radix sorting over null values.
   */
  private int nullBoundaryPos = 0;

  /*
   * How many records could be inserted, because part of the array should be left for sorting.
   */
  private int usableCapacity = 0;

  private long initialSize;

  private long totalSortTimeNanos = 0L;

  public UnsafeInMemoryRadixSorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final List<PrefixComparator> prefixComparators,
    int initialSize,
    boolean canUseRadixSort) {
    this(consumer, memoryManager, recordComparator, prefixComparators,
      consumer.allocateArray(initialSize * 2L), canUseRadixSort);
  }

  public UnsafeInMemoryRadixSorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final List<PrefixComparator> prefixComparators,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    if (recordComparator != null) {
      if (canUseRadixSort ) {
        this.radixSortSupports = prefixComparators.stream()
                .map(prefixComparator->((PrefixComparators.RadixSortSupport)prefixComparator))
                .collect(Collectors.toList());
      } else {
        this.radixSortSupports = null;
      }
    } else {
      this.radixSortSupports = null;
    }
    this.array = array;
    this.usableCapacity = getUsableCapacity();
  }

  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    return (int) (array.size() / (radixSortSupports != null ? 2 : 1.5));
  }

  public long getInitialSize() {
    return initialSize;
  }

  /**
   * Free the memory used by pointer array.
   */
  public void freeMemory() {
    if (consumer != null) {
      if (array != null) {
        consumer.freeArray(array);
      }

      // Set the array to null instead of allocating a new array. Allocating an array could have
      // triggered another spill and this method already is called from UnsafeExternalSorter when
      // spilling. Attempting to allocate while spilling is dangerous, as we could be holding onto
      // a large partially complete allocation, which may prevent other memory from being allocated.
      // Instead we will allocate the new array when it is necessary.
      array = null;
      usableCapacity = 0;
    }
    pos = 0;
    nullBoundaryPos = 0;
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 3;
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  public long getMemoryUsage() {
    if (array == null) {
      return 0L;
    }

    return array.size() * 8;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < usableCapacity;
  }

  public void expandPointerArray(LongArray newArray) {
    if (array != null) {
      if (newArray.size() < array.size()) {
        // checkstyle.off: RegexpSinglelineJava
        throw new SparkOutOfMemoryError("Not enough memory to grow pointer array");
        // checkstyle.on: RegexpSinglelineJava
      }
      Platform.copyMemory(
        array.getBaseObject(),
        array.getBaseOffset(),
        newArray.getBaseObject(),
        newArray.getBaseOffset(),
        pos * 8L);
      consumer.freeArray(array);
    }
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a uaoSize(4 or 8) bytes integer, followed by the record's bytes.
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix1 a user-defined key prefix
   */
  public void insertRecord(long recordPointer,
                           long keyPrefix1, boolean prefix1IsNull,
                           long keyPrefix2, boolean prefix2IsNull) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    assert radixSortSupports != null;
    boolean prefix2Desc = radixSortSupports.get(1).sortDescending();
    if (prefix1IsNull) {
      // Swap forward a non-null record to make room for this one at the beginning of the array.
      array.set(pos, array.get(nullBoundaryPos));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 1));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 2));
      pos++;

      // Place this record in the vacated position.
      array.set(nullBoundaryPos, recordPointer);
      nullBoundaryPos++;
      array.set(nullBoundaryPos, keyPrefix1);
      nullBoundaryPos++;
      //  prefix2是null的情况
      if(prefix2Desc){
      array.set(nullBoundaryPos, Long.MAX_VALUE-keyPrefix2);
      }else{
        array.set(nullBoundaryPos, keyPrefix2);
      }
      nullBoundaryPos++;
    } else {
      // 行记录位置
      array.set(pos, recordPointer);
      pos++;
      //  修改2，前缀，这里放的时候需要放2个
      array.set(pos, keyPrefix1);
      pos++;
       if(prefix2Desc) {
           array.set(pos, Long.MAX_VALUE - keyPrefix2);
       }else{
           array.set(pos, keyPrefix2);
       }
      pos++;
    }
  }

  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;

    private long keyPrefix2;
    private int recordLength;
    private long currentPageNumber;
    private final TaskContext taskContext = TaskContext.get();

    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
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
      return position / 3 < numRecords;
    }

    @Override
    public void loadNext() {
      // Kill the task in case it has been marked as killed. This logic is from
      // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead. This check is added here in `loadNext()` instead of in
      // `hasNext()` because it's technically possible for the caller to be relying on
      // `getNumRecords()` instead of `hasNext()` to know when to stop.
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
      keyPrefix2 = array.get(offset + position + 2);
      position += 3;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public long getCurrentPageNumber() {
      return currentPageNumber;
    }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public UnsafeSorterIterator getSortedIterator() {
    if (numRecords() == 0) {
      // `array` might be null, so make sure that it is not accessed by returning early.
      return new SortedIterator(0, 0);
    }

    int offset = 0;
    long start = System.nanoTime();
      if (this.radixSortSupports != null) {
        // 拿到排完序后的数据在数组中的结束位置
        offset = RadixTwoColumnSort.sortKeyPrefixArray(
          array, nullBoundaryPos, (pos - nullBoundaryPos) / 3L, 0, 15,
          radixSortSupports.get(0).sortDescending(), radixSortSupports.get(0).sortSigned(), 8);

/*        offset = RadixTwoColumnSort.sortKeyPrefixArray(
                array, nullBoundaryPos, (pos - nullBoundaryPos) / 3L, 0, 7,
                radixSortSupports.get(1).sortDescending(), radixSortSupports.get(1).sortSigned(),16);*/
      }

    totalSortTimeNanos += System.nanoTime() - start;
    if (nullBoundaryPos > 0) {
      assert radixSortSupports != null : "Nulls are only stored separately with radix sort";
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();

      // The null order is either LAST or FIRST, regardless of sorting direction (ASC|DESC)

      if (radixSortSupports.get(0).nullsFirst()) {
        queue.add(new SortedIterator(nullBoundaryPos / 3, 0));
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 3, offset));
      } else {
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 3, offset));
        queue.add(new SortedIterator(nullBoundaryPos / 3, 0));
      }
      return new UnsafeExternalSorter.ChainedIterator(queue);
    } else {
      return new SortedIterator(pos / 3, offset);
    }
  }
}
