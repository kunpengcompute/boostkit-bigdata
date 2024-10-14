/*
 * Copyright (C) 2021-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution.datasources.parquet;

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchScanReader;
import com.huawei.boostkit.spark.timestamp.LegacyBehaviorPolicy;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OmniColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.*;

/**
 * To support parquet file format in native, ParquetColumnarBatchScanReader uses ParquetColumnarBatchJniReader to
 * read data and return batch to next operator.
 */
public class OmniParquetColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;
  private ColumnarBatch columnarBatch;
  private ParquetColumnarBatchScanReader reader;
  private org.apache.spark.sql.vectorized.ColumnVector[] wrap;

  // Store the immutable cols, such as partionCols and misingCols, which only init once.
  // And wrap will slice vecs from templateWrap when calling nextBatch().
  private org.apache.spark.sql.vectorized.ColumnVector[] templateWrap;
  private Vec[] vecs;

  private List<DataType> types = new ArrayList<>();

  private StructType requiredSchema;
  private Filter pushedFilter;

  private final LegacyBehaviorPolicy datetimeRebaseMode;

  private final LegacyBehaviorPolicy int96RebaseMode;

  private final List<Type> parquetTypes;

  public OmniParquetColumnarBatchReader(int capacity, StructType requiredSchema, Filter pushedFilter,
                                        String datetimeRebaseMode, String int96RebaseMode, List<Type> parquetTypes) {
    this.capacity = capacity;
    this.requiredSchema = requiredSchema;
    this.pushedFilter = pushedFilter;
    this.datetimeRebaseMode = LegacyBehaviorPolicy.valueOf(datetimeRebaseMode);
    this.int96RebaseMode = LegacyBehaviorPolicy.valueOf(int96RebaseMode);
    this.parquetTypes = parquetTypes;
  }

  public ParquetColumnarBatchScanReader getReader() {
    return this.reader;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    // Free vecs from templateWrap.
    for (int i = 0; i < templateWrap.length; i++) {
      org.apache.spark.sql.vectorized.ColumnVector vector = templateWrap[i];
      if (vector != null) {
        ((OmniColumnVector) vector).close();
      }
    }
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    ParquetInputSplit split = (ParquetInputSplit)inputSplit;
    this.reader = new ParquetColumnarBatchScanReader(requiredSchema, datetimeRebaseMode, int96RebaseMode, parquetTypes);
    reader.initializeReaderJava(split.getPath(), split.getStart(), split.getEnd(),
            capacity, pushedFilter);
  }


  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    StructType batchSchema = new StructType();
    for (StructField f: requiredSchema.fields()) {
      batchSchema = batchSchema.add(f);
      types.add(f.dataType());
    }
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
    }
    wrap = new org.apache.spark.sql.vectorized.ColumnVector[batchSchema.length()];
    columnarBatch = new ColumnarBatch(wrap);
    // Init template also
    templateWrap = new org.apache.spark.sql.vectorized.ColumnVector[batchSchema.length()];
    // Init partition columns
    if (partitionColumns != null) {
      int partitionIdx = requiredSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        OmniColumnVector partitionCol = new OmniColumnVector(capacity, partitionColumns.fields()[i].dataType(), true);
        OmniColumnVectorUtils.populate(partitionCol, partitionValues, i);
        partitionCol.setIsConstant();
        // templateWrap always stores partitionCol
        templateWrap[i + partitionIdx] = partitionCol;
        // wrap also need to new partitionCol but not init vec
        wrap[i + partitionIdx] = new OmniColumnVector(capacity, partitionColumns.fields()[i].dataType(), false);
      }
    }


    // Init columns that need read from native
    for (int i = 0; i < requiredSchema.fields().length; i++) {
      templateWrap[i] = new OmniColumnVector(capacity, requiredSchema.fields()[i].dataType(), false);
      wrap[i] = new OmniColumnVector(capacity, requiredSchema.fields()[i].dataType(), false);
    }

    vecs = new Vec[types.size()];
  }

  /**
   * Advance to the next batch of rows. Return false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    int batchSize = reader.next(vecs, types);
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < types.size(); i++) {
        ((OmniColumnVector) wrap[i]).setVec(vecs[i]);
    }

    try {
      // Slice other vecs from templateWrap.
      for (int i = 0; i < templateWrap.length; i++) {
        OmniColumnVector vector = (OmniColumnVector) templateWrap[i];
        if (vector.isConstant()) {
          ((OmniColumnVector) wrap[i]).setVec(vector.getVec().slice(0, batchSize));
        }
      }
    } catch (Exception e) {
      for (Vec vec : vecs) {
        vec.close();
      }

      for (int i = 0; i < templateWrap.length; i++) {
        org.apache.spark.sql.vectorized.ColumnVector vector = templateWrap[i];
        if (vector != null) {
          ((OmniColumnVector) vector).close();
          templateWrap[i] = null;
        }
      }

      throw new RuntimeException(e);
    }
    return true;
  }

}
