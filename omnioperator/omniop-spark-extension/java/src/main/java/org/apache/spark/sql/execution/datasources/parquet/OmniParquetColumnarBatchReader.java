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

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchJniReader;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * To support parquet file format in native, OmniParquetColumnarBatchReader uses ParquetColumnarBatchJniReader to
 * read data and return batch to next operator.
 */
public class OmniParquetColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;
  private FilterCompat.Filter filter;
  private ParquetMetadata fileFooter;
  private boolean[] missingColumns;
  private ColumnarBatch columnarBatch;
  private MessageType fileSchema;
  private MessageType requestedSchema;
  private StructType sparkSchema;
  private ParquetColumnarBatchJniReader reader;
  private org.apache.spark.sql.vectorized.ColumnVector[] wrap;

  // Store the immutable cols, such as partionCols and misingCols, which only init once.
  // And wrap will slice vecs from templateWrap when calling nextBatch().
  private org.apache.spark.sql.vectorized.ColumnVector[] templateWrap;
  private Vec[] vecs;
  private boolean isFilterPredicate = false;

  public OmniParquetColumnarBatchReader(int capacity, ParquetMetadata fileFooter) {
    this.capacity = capacity;
    this.fileFooter = fileFooter;
  }

  public ParquetColumnarBatchJniReader getReader() {
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
      OmniColumnVector vector = (OmniColumnVector) templateWrap[i];
      vector.close();
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
    Configuration configuration = taskAttemptContext.getConfiguration();
    ParquetInputSplit split = (ParquetInputSplit)inputSplit;

    this.filter = getFilter(configuration);
    this.isFilterPredicate = filter instanceof FilterCompat.FilterPredicateCompat ? true : false;

    this.fileSchema = fileFooter.getFileMetaData().getSchema();
    Map<String, String> fileMetadata = fileFooter.getFileMetaData().getKeyValueMetaData();
    ReadSupport<ColumnarBatch> readSupport = getReadSupportInstance(getReadSupportClass(configuration));
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
            taskAttemptContext.getConfiguration(), toSetMultiMap(fileMetadata), fileSchema));
    this.requestedSchema = readContext.getRequestedSchema();
    String sparkRequestedSchemaString = configuration.get(ParquetReadSupport$.MODULE$.SPARK_ROW_REQUESTED_SCHEMA());
    this.sparkSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
    this.reader = new ParquetColumnarBatchJniReader();
    // PushDown rowGroups and columns indices for native reader.
    List<Integer> rowgroupIndices = getFilteredBlocks(split.getStart(), split.getEnd());
    List<Integer> columnIndices = getColumnIndices(requestedSchema.getColumns(), fileSchema.getColumns());
    String ugi = UserGroupInformation.getCurrentUser().toString();
    reader.initializeReaderJava(split.getPath().toString(), capacity, rowgroupIndices, columnIndices, ugi);
    // Add missing Cols flags.
    initializeInternal();
  }

  private List<Integer> getFilteredBlocks(long start, long end) throws IOException, InterruptedException {
    List<Integer> res = new ArrayList<>();
    List<BlockMetaData> blocks = fileFooter.getBlocks();
    for (int i = 0; i < blocks.size(); i++) {
      BlockMetaData block = blocks.get(i);
      long totalSize = 0;
      long startIndex = block.getStartingPos();
      for (ColumnChunkMetaData col : block.getColumns()) {
        totalSize += col.getTotalSize();
      }
      long midPoint = startIndex + totalSize / 2;
      if (midPoint >= start && midPoint < end) {
        if (isFilterPredicate) {
          boolean drop = StatisticsFilter.canDrop(((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate(),
            block.getColumns());
          if (!drop) {
            res.add(i);
          }
        } else {
          res.add(i);
        }
      }
    }
    return res;
  }

  private List<Integer> getColumnIndices(List<ColumnDescriptor> requestedColumns, List<ColumnDescriptor> allColumns) {
    List<Integer> res = new ArrayList<>();
    for (int i = 0; i < requestedColumns.size(); i++) {
      ColumnDescriptor it = requestedColumns.get(i);
      for (int j = 0; j < allColumns.size(); j++) {
        if (it.toString().equals(allColumns.get(j).toString())) {
          res.add(j);
          break;
        }
      }
    }

    if (res.size() != requestedColumns.size()) {
      throw new ParquetDecodingException("Parquet mapping column indices error");
    }
    return res;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<String[]> paths = requestedSchema.getPaths();
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }

      String[] colPath = paths.get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " + Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    StructType batchSchema = new StructType();
    for (StructField f: sparkSchema.fields()) {
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
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        OmniColumnVector partitionCol = new OmniColumnVector(capacity, partitionColumns.fields()[i].dataType(), true);
        ColumnVectorUtils.populate(partitionCol, partitionValues, i);
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
        OmniColumnVector missingCol = new OmniColumnVector(capacity, sparkSchema.fields()[i].dataType(), true);
        missingCol.putNulls(0, capacity);
        missingCol.setIsConstant();
        templateWrap[i] = missingCol;
      } else {
        templateWrap[i] = new OmniColumnVector(capacity, sparkSchema.fields()[i].dataType(), false);
      }

      // wrap also need to new partitionCol but not init vec
      wrap[i] = new OmniColumnVector(capacity, sparkSchema.fields()[i].dataType(), false);
    }
    vecs = new Vec[requestedSchema.getFieldCount()];
  }

  /**
   * Advance to the next batch of rows. Return false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    int batchSize = reader.next(vecs);
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requestedSchema.getFieldCount(); i++) {
      if (!missingColumns[i]) {
        ((OmniColumnVector) wrap[i]).setVec(vecs[i]);
      }
    }

    // Slice other vecs from templateWrap.
    for (int i = 0; i < templateWrap.length; i++) {
      OmniColumnVector vector = (OmniColumnVector) templateWrap[i];
      if (vector.isConstant()) {
        ((OmniColumnVector) wrap[i]).setVec(vector.getVec().slice(0, batchSize));
      }
    }
    return true;
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends ReadSupport<ColumnarBatch>> getReadSupportClass(Configuration configuration) {
    return (Class<? extends ReadSupport<ColumnarBatch>>) ConfigurationUtil.getClassFromConfig(configuration,
            ParquetInputFormat.READ_SUPPORT_CLASS, ReadSupport.class);
  }

  /**
   * @param readSupportClass to instantiate
   * @return the configured read support
   */
  private static <T> ReadSupport<T> getReadSupportInstance(Class<? extends ReadSupport<T>> readSupportClass) {
    try {
      return readSupportClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    }
  }
}
