/*
 * Copyright (C) 2021-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution.datasources.orc;

import com.google.common.annotations.VisibleForTesting;
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor;
import com.huawei.boostkit.spark.jni.OrcColumnarBatchScanReader;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OmniColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.ArrayList;

/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
public class OmniOrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;

  // Native Record reader from ORC row batch.
  private OrcColumnarBatchScanReader recordReader;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  @VisibleForTesting
  public ColumnarBatch columnarBatch;

  // The wrapped ORC column vectors.
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  private org.apache.spark.sql.vectorized.ColumnVector[] templateWrappers;

  private Vec[] vecs;

  private int[] vecTypeIds;

  private StructType requiredSchema;
  private Filter pushedFilter;

  public OmniOrcColumnarBatchReader(int capacity, StructType requiredSchema, Filter pushedFilter) {
    this.capacity = capacity;
    this.requiredSchema = requiredSchema;
    this.pushedFilter = pushedFilter;
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
  public float getProgress() throws IOException {
    return recordReader.getProgress();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }

    // Free vecs from templateWrappers.
    for (int i = 0; i < templateWrappers.length; i++) {
      org.apache.spark.sql.vectorized.ColumnVector vector = templateWrappers[i];
      if (vector != null) {
        ((OmniColumnVector) vector).close();
      }
    }
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   */
  @Override
  public void initialize(
          InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    recordReader = new OrcColumnarBatchScanReader();
    recordReader.initializeReaderJava(fileSplit.getPath().toUri());

    initDataColIds();
    recordReader.initializeRecordReaderJava(fileSplit.getStart(), fileSplit.getLength(), pushedFilter, requiredSchema);
  }

  private void initDataColIds() {
    // find requiredS fieldNames
    String[] requiredfieldNames = requiredSchema.fieldNames();
    // save valid cols and numbers of valid cols
    recordReader.colsToGet = new int[requiredfieldNames.length];
    recordReader.includedColumns = new ArrayList<>();
    // collect read cols types
    ArrayList<Integer> typeBuilder = new ArrayList<>();

    for (int i = 0; i < requiredfieldNames.length; i++) {
      String target = requiredfieldNames[i];
      // if not find, set colsToGet value -1, else set colsToGet 0
      if (recordReader.allFieldsNames.contains(target)) {
        recordReader.colsToGet[i] = 0;
        recordReader.includedColumns.add(requiredfieldNames[i]);
        typeBuilder.add(OmniExpressionAdaptor.sparkTypeToOmniType(requiredSchema.fields()[i].dataType()));
      } else {
        recordReader.colsToGet[i] = -1;
      }
    }

    vecTypeIds = typeBuilder.stream().mapToInt(Integer::intValue).toArray();
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   *
   * @param partitionColumns partition columns
   * @param partitionValues Values of partition columns.
   */
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    StructType resultSchema = new StructType();

    for (StructField f: requiredSchema.fields()) {
      resultSchema = resultSchema.add(f);
    }

    if (partitionColumns != null) {
      for (StructField f: partitionColumns.fields()) {
        resultSchema = resultSchema.add(f);
      }
    }

    // Just wrap the ORC column vector instead of copying it to Spark column vector.
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    templateWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    if (partitionColumns != null) {
      int partitionIdx = requiredSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        OmniColumnVector partitionCol = new OmniColumnVector(capacity, partitionColumns.fields()[i].dataType(), true);
        OmniColumnVectorUtils.populate(partitionCol, partitionValues, i);
        partitionCol.setIsConstant();
        templateWrappers[i + partitionIdx] = partitionCol;
        orcVectorWrappers[i + partitionIdx] = new OmniColumnVector(capacity, partitionColumns.fields()[i].dataType(), false);
      }
    }

    for (int i = 0; i < requiredSchema.fields().length; i++) {
      DataType dt = requiredSchema.fields()[i].dataType();
      if (recordReader.colsToGet[i] == -1) {
        // missing cols
        OmniColumnVector missingCol = new OmniColumnVector(capacity, dt, true);
        missingCol.putNulls(0, capacity);
        missingCol.setIsConstant();
        templateWrappers[i] = missingCol;
      } else {
        templateWrappers[i] = new OmniColumnVector(capacity, dt, false);
      }
      orcVectorWrappers[i] = new OmniColumnVector(capacity, dt, false);
    }

    // init batch
    recordReader.initBatchJava(capacity);
    vecs = new Vec[orcVectorWrappers.length];
    columnarBatch = new ColumnarBatch(orcVectorWrappers);
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private boolean nextBatch() throws IOException {
    int batchSize;
    if (requiredSchema.fields().length == 0 || vecTypeIds.length == 0) {
      batchSize = (int) recordReader.getNumberOfRowsJava();
    } else {
      batchSize = recordReader.next(vecs, vecTypeIds);
    }
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requiredSchema.fields().length; i++) {
      if (recordReader.colsToGet[i] != -1) {
        ((OmniColumnVector) orcVectorWrappers[i]).setVec(vecs[i]);
      }
    }

    try {
      // Slice other vecs from templateWrap.
      for (int i = 0; i < templateWrappers.length; i++) {
        OmniColumnVector vector = (OmniColumnVector) templateWrappers[i];
        if (vector.isConstant()) {
          ((OmniColumnVector) orcVectorWrappers[i]).setVec(vector.getVec().slice(0, batchSize));
        }
      }
    } catch (Exception e) {
      for (Vec vec : vecs) {
        vec.close();
      }

      for (int i = 0; i < templateWrappers.length; i++) {
        org.apache.spark.sql.vectorized.ColumnVector vector = templateWrappers[i];
        if (vector != null) {
          ((OmniColumnVector) vector).close();
          templateWrappers[i] = null;
        }
      }

      throw new RuntimeException(e);
    }

    return true;
  }
}
