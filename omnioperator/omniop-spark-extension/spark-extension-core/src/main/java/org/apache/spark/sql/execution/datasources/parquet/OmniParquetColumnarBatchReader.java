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

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor;
import com.huawei.boostkit.spark.jni.ParquetColumnarBatchScanReader;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec;
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
  private boolean[] missingColumns;
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

  private final RebaseSpec datetimeRebaseSpec;

  private final RebaseSpec int96RebaseSpec;

  private final List<Type> parquetTypes;

  public OmniParquetColumnarBatchReader(int capacity, StructType requiredSchema, Filter pushedFilter,
                                        RebaseSpec datetimeRebaseSpec, RebaseSpec int96RebaseSpec, List<Type> parquetTypes) {
    this.capacity = capacity;
    this.requiredSchema = requiredSchema;
    this.pushedFilter = pushedFilter;
    this.datetimeRebaseSpec = datetimeRebaseSpec;
    this.int96RebaseSpec = int96RebaseSpec;
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
    this.reader = new ParquetColumnarBatchScanReader(requiredSchema, datetimeRebaseSpec, int96RebaseSpec, parquetTypes);
    reader.initializeReaderJava(split.getPath(), capacity);
    String[] includeFieldNames = initializeInternal();
    reader.initializeRecordReaderJava(split.getStart(), split.getEnd(), includeFieldNames, pushedFilter);
  }

  private String[] initializeInternal() throws IOException, UnsupportedOperationException {
    String[] requiredFieldNames = requiredSchema.fieldNames();
    StructField[] structFields = requiredSchema.fields();

    missingColumns = new boolean[requiredFieldNames.length];
    ArrayList<String> allFieldsNames = reader.getAllFieldsNames();

    ArrayList<String> includeFieldNames = new ArrayList<>();
    for (int i = 0; i < requiredFieldNames.length; i++) {
      String target = requiredFieldNames[i];
      if (allFieldsNames.contains(target)) {
        missingColumns[i] = false;
        includeFieldNames.add(target);
        types.add(structFields[i].dataType());
      } else {
        missingColumns[i] = true;
      }
    }
    return includeFieldNames.toArray(new String[includeFieldNames.size()]);
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

    // Initialize missing columns with nulls.
    for (int i = 0; i < missingColumns.length; i++) {
      // templateWrap always stores missingCol. For other requested cols from native, it will not init them.
      if (missingColumns[i]) {
        OmniColumnVector missingCol = new OmniColumnVector(capacity, requiredSchema.fields()[i].dataType(), true);
        missingCol.putNulls(0, capacity);
        missingCol.setIsConstant();
        templateWrap[i] = missingCol;
      } else {
        templateWrap[i] = new OmniColumnVector(capacity, requiredSchema.fields()[i].dataType(), false);
      }

      // wrap also need to new partitionCol but not init vec
      wrap[i] = new OmniColumnVector(capacity, requiredSchema.fields()[i].dataType(), false);
    }
    vecs = new Vec[wrap.length];
  }

  /**
   * Advance to the next batch of rows. Return false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    int batchSize = reader.next(vecs, missingColumns, types);
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requiredSchema.fields().length; i++) {
      if (!missingColumns[i]) {
        ((OmniColumnVector) wrap[i]).setVec(vecs[i]);
      }
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
