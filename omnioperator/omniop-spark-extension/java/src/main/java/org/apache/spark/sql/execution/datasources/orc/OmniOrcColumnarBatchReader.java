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
import com.huawei.boostkit.spark.jni.OrcColumnarBatchJniReader;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 * After creating, `initialize` and `initBatch` should be called sequentially.
 */
public class OmniOrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.

  private int capacity;

  /**
   * The column IDs of the physical ORC file schema which are required by this reader.
   * -1 means this required column is partition column, or it doesn't exist in the ORC file.
   * Ideally partition column should never appear in the physical file, and should only appear
   * in the directory name. However, Spark allows partition columns inside physical file,
   * but Spark will discard the values from the file, and use the partition value got from
   * directory name. The column order will be reserved though.
   */
  @VisibleForTesting
  public int[] requestedDataColIds;

  // Native Record reader from ORC row batch.
  private OrcColumnarBatchJniReader recordReader;

  private StructField[] requiredFields;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  @VisibleForTesting
  public ColumnarBatch columnarBatch;

  // The wrapped ORC column vectors.
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  private org.apache.spark.sql.vectorized.ColumnVector[] templateWrappers;

  private Vec[] vecs;

  public OmniOrcColumnarBatchReader(int capacity) {
    this.capacity = capacity;
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
      OmniColumnVector vector = (OmniColumnVector) templateWrappers[i];
      vector.close();
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
    Configuration conf = taskAttemptContext.getConfiguration();
    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        .filesystem(fileSplit.getPath().getFileSystem(conf));
//  long reader = OrcColumnarNativeReader.initializeReaderJava(fileSplit.getPath(), readerOptions);
    Reader.Options options =
      OrcColumnarNativeReader.buildOptions(conf, fileSplit.getStart(), fileSplit.getLength());
    recordReader = new OrcColumnarBatchJniReader();
    recordReader.initializeReaderJava(fileSplit.getPath().toString(), readerOptions);
    recordReader.initializeRecordReaderJava(options);
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   *
   * @param orcSchema Schema from ORC file reader.
   * @param requiredFields All the fields that are required to return, including partition fields.
   * @param requestedDataColIds Requested column ids from orcSchema. -1 if not existed.
   * @param requestedPartitionColIds Requested column ids from partition schema. -1 if not existed.
   * @param partitionValues Values of partition columns.
   */
  public void initBatch(
      TypeDescription orcSchema,
      StructField[] requiredFields,
      int[] requestedDataColIds,
      int[] requestedPartitionColIds,
      InternalRow partitionValues) {
    // wrap = new OrcShimUtils.VectorizedRowBatchWrap(orcSchema.createRowBatch(capacity));
    // assert(!wrap.batch().selectedInUse); // `selectedInUse` should be initialized with `false`.
    assert(requiredFields.length == requestedDataColIds.length);
    assert(requiredFields.length == requestedPartitionColIds.length);
    // If a required column is also partition column, use partition value and don't read from file.
    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedPartitionColIds[i] != -1) {
        requestedDataColIds[i] = -1;
      }
    }
    this.requiredFields = requiredFields;
    this.requestedDataColIds = requestedDataColIds;

    StructType resultSchema = new StructType(requiredFields);

    // Just wrap the ORC column vector instead of copying it to Spark column vector.
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    templateWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    for (int i = 0; i < requiredFields.length; i++) {
      DataType dt = requiredFields[i].dataType();
      if (requestedPartitionColIds[i] != -1) {
        OmniColumnVector partitionCol = new OmniColumnVector(capacity, dt, true);
        ColumnVectorUtils.populate(partitionCol, partitionValues, requestedPartitionColIds[i]);
        partitionCol.setIsConstant();
        templateWrappers[i] = partitionCol;
        orcVectorWrappers[i] = new OmniColumnVector(capacity, dt, false);;
      } else {
        int colId = requestedDataColIds[i];
        // Initialize the missing columns once.
        if (colId == -1) {
          OmniColumnVector missingCol = new OmniColumnVector(capacity, dt, true);
          missingCol.putNulls(0, capacity);
          missingCol.setIsConstant();
          templateWrappers[i] = missingCol;
        } else {
          templateWrappers[i] = new OmniColumnVector(capacity, dt, false);
        }
        orcVectorWrappers[i] = new OmniColumnVector(capacity, dt, false);
      }
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
     int batchSize = capacity;
    if ((requiredFields.length == 1 && requestedDataColIds[0] == -1) || requiredFields.length == 0) {
        batchSize = (int) recordReader.getNumberOfRowsJava();
    } else {
        batchSize = recordReader.next(vecs);
    }
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedDataColIds[i] != -1) {
          int colId = requestedDataColIds[i];
        ((OmniColumnVector) orcVectorWrappers[i]).setVec(vecs[colId]);
      }
    }

    // Slice other vecs from templateWrap.
    for (int i = 0; i < templateWrappers.length; i++) {
      OmniColumnVector vector = (OmniColumnVector) templateWrappers[i];
      if (vector.isConstant()) {
        ((OmniColumnVector) orcVectorWrappers[i]).setVec(vector.getVec().slice(0, batchSize));
      }
    }
    return true;
  }
}
