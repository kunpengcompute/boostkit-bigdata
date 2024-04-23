/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive.reader;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

import com.google.common.base.Strings;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.ShortDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OmniParquetRecordReader
        implements RecordReader<NullWritable, VecBatchWrapper>, StatsProvidingRecordReader {
    public static final Logger LOG = LoggerFactory.getLogger(OmniParquetRecordReader.class);

    private static final Map<OriginalType, DataType> PARQUET_TO_OMNI_TYPE = new HashMap<OriginalType, DataType>() {
        {
            put(OriginalType.DATE, IntDataType.INTEGER);
            put(OriginalType.INT_8, ShortDataType.SHORT);
            put(OriginalType.INT_16, ShortDataType.SHORT);
            put(OriginalType.INT_32, IntDataType.INTEGER);
            put(OriginalType.INT_64, LongDataType.LONG);
            put(OriginalType.UINT_8, ShortDataType.SHORT);
            put(OriginalType.UINT_16, ShortDataType.SHORT);
            put(OriginalType.UINT_32, IntDataType.INTEGER);
            put(OriginalType.UINT_64, LongDataType.LONG);
            put(OriginalType.UTF8, VarcharDataType.VARCHAR);
        }
    };

    private static final Map<PrimitiveTypeName, DataType> NULL_ORIGINAL_TO_OMNI_TYPE = new HashMap<PrimitiveTypeName, DataType>() {
        {
            put(PrimitiveTypeName.INT32, IntDataType.INTEGER);
            put(PrimitiveTypeName.INT64, LongDataType.LONG);
            put(PrimitiveTypeName.DOUBLE, DoubleDataType.DOUBLE);
            put(PrimitiveTypeName.BOOLEAN, BooleanDataType.BOOLEAN);
            put(PrimitiveTypeName.BINARY, VarcharDataType.VARCHAR);
            put(PrimitiveTypeName.FLOAT, DoubleDataType.DOUBLE);
        }
    };

    protected ParquetColumnarBatchScanReader recordReader;
    protected Vec[] vecs;
    protected float progress = 0.0f;
    protected long splitLen; // for getPos()
    protected boolean skipTimestampConversion = false;
    protected SerDeStats serDeStats;
    protected JobConf jobConf;
    protected ProjectionPusher projectionPusher;

    protected FilterCompat.Filter filter;
    protected boolean isFilterPredicate = false;
    protected ParquetMetadata fileFooter;

    protected Operator tableScanOp;
    protected List<DataType> typeIds;
    protected MessageType fileSchema;
    protected List<Integer> included;
    protected ParquetInputSplit split;
    protected Configuration conf;

    OmniParquetRecordReader(InputSplit oldSplit, JobConf oldJobConf) throws IOException {
        this.splitLen = oldSplit.getLength();
        this.serDeStats = new SerDeStats();
        this.projectionPusher = new ProjectionPusher();
        jobConf = oldJobConf;
        this.conf = jobConf;
        this.fileFooter = ParquetFileReader.readFooter(oldJobConf, ((FileSplit) oldSplit).getPath());
        this.fileSchema = fileFooter.getFileMetaData().getSchema();
        this.included = ColumnProjectionUtils.getReadColumnIDs(conf);
        Map<String, String> fileMatadata = fileFooter.getFileMetaData().getKeyValueMetaData();
        MessageType schema = getRequriedSchema(this.fileSchema);
        List<ColumnDescriptor> columns = schema.getColumns();
        this.typeIds = new ArrayList<DataType>();
        for (int i = 0; i < columns.size(); i++) {
            PrimitiveType type = columns.get(i).getPrimitiveType();
            addTypeId(type);
        }
        this.split = getSplit(oldSplit, jobConf);
        // create a TaskInputOutputContext
        if (skipTimestampConversion
                ^ HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
            conf = new JobConf(oldJobConf);
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION,
                    skipTimestampConversion);
        }
        this.isFilterPredicate = filter instanceof FilterCompat.FilterPredicateCompat;
        this.recordReader = new ParquetColumnarBatchScanReader();
        // PushDown rowGroups and columns indices for native reader.
        List<Integer> rowgroupIndices = getFilteredBlocks(split.getStart(), split.getEnd());
        String ugi = UserGroupInformation.getCurrentUser().toString();
        Collections.sort(included);
        this.recordReader = new ParquetColumnarBatchScanReader();
        this.recordReader.initializeReaderJava(split.getPath(), BATCH, rowgroupIndices, included, ugi);
        this.vecs = new Vec[included.size()];
        this.serDeStats = new SerDeStats();
        Utilities.getMapWork(conf).getAliasToWork().values().forEach(op -> {
            if (op.getType().equals(OperatorType.TABLESCAN)) {
                this.tableScanOp = op;
            }
        });
    }

    private void addTypeId(PrimitiveType type) {
        if (type.getOriginalType() != null) {
            if (type.getOriginalType().name().equals("DECIMAL")) {
                // max presicion for decimal64 is 18
                if (type.getDecimalMetadata().getPrecision() > 18) {
                    typeIds.add(Decimal128DataType.DECIMAL128);
                } else {
                    typeIds.add(Decimal64DataType.DECIMAL64);
                }
            } else {
                typeIds.add(PARQUET_TO_OMNI_TYPE.get(type.getOriginalType()));
            }
        } else {
            typeIds.add(NULL_ORIGINAL_TO_OMNI_TYPE.get(type.getPrimitiveTypeName()));
        }
    }

    private MessageType getRequriedSchema(MessageType schema) {
        Set<Integer> requiredIds = new HashSet<>(this.included);
        List<Type> typeList = new ArrayList<Type>();
        for (int i = 0; i < schema.getFields().size(); i++) {
            if (requiredIds.contains(i)) {
                typeList.add(schema.getFields().get(i));
            }
        }
        return new MessageType(schema.getName(), typeList);
    }

    protected ParquetInputSplit getSplit(final org.apache.hadoop.mapred.InputSplit oldSplit, final JobConf conf)
            throws IOException {
        ParquetInputSplit split;
        if (oldSplit instanceof FileSplit) {
            final Path finalPath = ((FileSplit) oldSplit).getPath();
            final List<BlockMetaData> blocks = fileFooter.getBlocks();
            final FileMetaData fileMetaData = fileFooter.getFileMetaData();

            this.filter = setFilter(jobConf, fileMetaData.getSchema());
            final ReadSupport.ReadContext readContext = new DataWritableReadSupport()
                    .init(new InitContext(jobConf, null, fileMetaData.getSchema()));

            // Compute stats
            for (BlockMetaData bmd : blocks) {
                serDeStats.setRowCount(serDeStats.getRowCount() + bmd.getRowCount());
                serDeStats.setRawDataSize(serDeStats.getRawDataSize() + bmd.getTotalByteSize());
            }
            final List<BlockMetaData> splitGroup = new ArrayList<BlockMetaData>();
            final long splitStart = ((FileSplit) oldSplit).getStart();
            final long splitLength = ((FileSplit) oldSplit).getLength();
            for (final BlockMetaData block : blocks) {
                final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                    splitGroup.add(block);
                }
            }
            if (splitGroup.isEmpty()) {
                LOG.warn("Skipping split, could not find row group in: " + oldSplit);
                return null;
            }
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
                skipTimestampConversion = !Strings.nullToEmpty(fileMetaData.getCreatedBy()).startsWith("parquet-mr");
            }
            split = new ParquetInputSplit(finalPath, splitStart, splitLength, oldSplit.getLocations(), splitGroup,
                    fileMetaData.getSchema().toString(), fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(), readContext.getReadSupportMetadata());
            return split;
        } else {
            throw new IllegalArgumentException("Unknown split type: " + oldSplit);
        }
    }

    private FilterCompat.Filter setFilter(final JobConf conf, MessageType schema) {
        SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
        if (sarg == null) {
            return null;
        }
        // Create the Parquet FilterPredicate without including columns that do not
        // exist
        // on the schema (such as partition columns).
        FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
        if (p != null) {
            // Filter may have sensitive information. Do not send to debug.
            LOG.debug("PARQUET predicate push down generated.");
            ParquetInputFormat.setFilterPredicate(conf, p);
            return FilterCompat.get(p);
        } else {
            // Filter may have sensitive information. Do not send to debug.
            LOG.debug("No PARQUET predicate push down is generated.");
            return null;
        }
    }

    private List<Integer> getFilteredBlocks(long start, long end) throws IOException {
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
                    boolean drop = StatisticsFilter.canDrop(
                            ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate(), block.getColumns());
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

    @Override
    public boolean next(NullWritable key, VecBatchWrapper value) throws IOException {
        int batchSize = BATCH;
        if (tableScanOp != null && tableScanOp.getDone()) {
            return false;
        }
        batchSize = recordReader.next(vecs, typeIds);
        if (batchSize == 0) {
            return false;
        }
        value.setVecBatch(new VecBatch(vecs, batchSize));
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public VecBatchWrapper createValue() {
        return new VecBatchWrapper();
    }

    @Override
    public long getPos() throws IOException {
        return (long) (splitLen * getProgress());
    }

    @Override
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
            recordReader = null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        return progress;
    }

    @Override
    public SerDeStats getStats() {
        return serDeStats;
    }
}