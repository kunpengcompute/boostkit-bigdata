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

import com.huawei.boostkit.hive.OmniTableScanOperator;
import com.huawei.boostkit.hive.expression.TypeUtils;
import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.ShortDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.ColumnDescriptor;
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
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OmniParquetRecordReader
        implements RecordReader<NullWritable, VecBatchWrapper>, StatsProvidingRecordReader {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParquetRecordReader.class);

    private static final Map<OriginalType, DataType> PARQUET_TO_OMNI_TYPE = new HashMap<OriginalType, DataType>() {
        {
            put(OriginalType.DATE, IntDataType.INTEGER);
            put(OriginalType.INT_8, ShortDataType.SHORT);
            put(OriginalType.INT_16, ShortDataType.SHORT);
            put(OriginalType.INT_32, IntDataType.INTEGER);
            put(OriginalType.INT_64, LongDataType.LONG);
            put(OriginalType.UINT_8, IntDataType.INTEGER);
            put(OriginalType.UINT_16, IntDataType.INTEGER);
            put(OriginalType.UINT_32, IntDataType.INTEGER);
            put(OriginalType.UINT_64, LongDataType.LONG);
            put(OriginalType.UTF8, VarcharDataType.VARCHAR);
        }
    };

    private static final Map<PrimitiveTypeName, DataType> NULL_ORIGINAL_TO_OMNI_TYPE =
            new HashMap<PrimitiveTypeName, DataType>() {
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
    protected boolean shouldSkipTimestampConversion = false;
    protected SerDeStats serDeStats;
    protected JobConf jobConf;
    protected ProjectionPusher projectionPusher;

    protected FilterCompat.Filter filter;
    protected boolean isFilterPredicate = false;
    protected ParquetMetadata fileFooter;

    protected Operator tableScanOp;
    protected List<DataType> typeIds;
    protected MessageType fileSchema;
    protected ParquetInputSplit split;
    protected Configuration conf;
    protected List<Integer> includedIds;
    protected List<String> includedNames;
    protected List<Type> fields;
    protected List<DataType> neededTypes;
    protected boolean[] missingColumns;

    OmniParquetRecordReader(InputSplit oldSplit, JobConf oldJobConf) throws IOException {
        this.splitLen = oldSplit.getLength();
        this.serDeStats = new SerDeStats();
        this.projectionPusher = new ProjectionPusher();
        jobConf = oldJobConf;
        this.conf = jobConf;
        this.fileFooter = ParquetFileReader.readFooter(oldJobConf, ((FileSplit) oldSplit).getPath());
        this.fileSchema = fileFooter.getFileMetaData().getSchema();
        this.includedIds = ColumnProjectionUtils.getReadColumnIDs(conf);
        this.includedNames = Arrays.asList(ColumnProjectionUtils.getReadColumnNames(conf));
        Map<String, String> fileMatadata = fileFooter.getFileMetaData().getKeyValueMetaData();
        MessageType schema = getRequriedSchema(this.fileSchema);
        this.fields = schema.getFields();
        this.split = getSplit(oldSplit, jobConf);
        // create a TaskInputOutputContext
        if (shouldSkipTimestampConversion
                ^ HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
            conf = new JobConf(oldJobConf);
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION,
                    shouldSkipTimestampConversion);
        }
        this.isFilterPredicate = filter instanceof FilterCompat.FilterPredicateCompat;
        // PushDown rowGroups and columns indices for native reader.
        List<Integer> rowgroupIndices = getFilteredBlocks(split.getStart(), split.getEnd());
        String ugi = UserGroupInformation.getCurrentUser().toString();
        this.recordReader = new ParquetColumnarBatchScanReader();
        this.recordReader.initializeReaderJava(split.getPath(), BATCH, ugi);
        String[] includeFieldNames = initializeInternal(schema.getColumns());
        this.recordReader.initializeRecordReaderJava(split.getStart(), split.getEnd(), includeFieldNames);
        this.vecs = new Vec[includedIds.size()];
        this.serDeStats = new SerDeStats();
        Utilities.getMapWork(conf).getAliasToWork().values().forEach(op -> {
            if (op.getType().equals(OperatorType.TABLESCAN)) {
                this.tableScanOp = op;
            }
        });
    }

    private String[] initializeInternal(List<ColumnDescriptor> columns) throws IOException, UnsupportedOperationException {
        missingColumns = new boolean[includedIds.size()];
        ArrayList<String> allFieldsNames = recordReader.getAllFieldsNames();
        this.typeIds = new ArrayList<DataType>();
        List<String> includeFieldNames = new ArrayList<>();
        for (int i = 0; i < includedIds.size(); i++) {
            String target = includedNames.get(i);
            if (allFieldsNames.contains(target)) {
                missingColumns[i] = false;
                includeFieldNames.add(target);
                addTypeId(columns.get(i).getPrimitiveType());
            } else {
                missingColumns[i] = true;
            }
        }
        return includeFieldNames.toArray(new String[includeFieldNames.size()]);
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
        List<Type> typeList = new ArrayList<Type>();
        List<Type> schemaFields = schema.getFields();
        for (Type schemaField : schemaFields) {
            if (includedNames.contains(schemaField.getName())) {
                typeList.add(schemaField);
            }
        }
        return new MessageType(schema.getName(), typeList);
    }

    protected ParquetInputSplit getSplit(final org.apache.hadoop.mapred.InputSplit oldSplit, final JobConf conf)
            throws IOException {
        ParquetInputSplit split;
        if (oldSplit instanceof FileSplit) {
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
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
                shouldSkipTimestampConversion = !Strings.nullToEmpty(
                        fileMetaData.getCreatedBy()).startsWith("parquet-mr");
            }
            final Path finalPath = ((FileSplit) oldSplit).getPath();
            split = new ParquetInputSplit(finalPath, splitStart, splitStart + splitLength, splitLength, null, null);
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
            long totalSize = 0L;
            long startIndex = block.getStartingPos();
            for (ColumnChunkMetaData col : block.getColumns()) {
                totalSize += col.getTotalSize();
            }
            long midPoint = startIndex + totalSize / 2;
            if (midPoint >= start && midPoint < end) {
                if (isFilterPredicate) {
                    boolean canDrop = StatisticsFilter.canDrop(
                            ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate(), block.getColumns());
                    if (!canDrop) {
                        res.add(i);
                    }
                } else {
                    res.add(i);
                }
            }
        }
        return res;
    }

    /**
     * getSignedIntValue
     *
     * @param str      value in string format
     * @return Integer value in int format
     */
    public Integer getSignedIntValue(String str) {
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * getSignedLongValue
     *
     * @param str   value in string format
     * @return Long value in Long format
     */
    public Long getSignedLongValue(String str) {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * convertUnsignedInt
     *
     * @param vec           unsigned int data is stored in IntVec
     * @param isDecimal     if table structure is decimal
     * @param decimalScale  pow of 10, which is used to obtain the true value of decimal
     * @return Vec          vector of the true data type
     */
    public Vec convertUnsignedInt(IntVec vec, boolean isDecimal, BigInteger decimalScale) {
        if (isDecimal) {
            Decimal128Vec deciVec = new Decimal128Vec(vec.getSize());
            for (int j = 0; j < vec.getSize(); j++) {
                if (vec.isNull(j) || getSignedIntValue(Integer.toUnsignedString(vec.get(j))) == null) {
                    deciVec.setNull(j);
                    continue;
                }
                BigInteger trueValue = new BigInteger(getSignedIntValue(Integer.toUnsignedString(vec.get(j)))
                        .toString());
                trueValue = trueValue.multiply(decimalScale);
                if (trueValue == null || trueValue.compareTo(BigInteger.ZERO) < 0) {
                    deciVec.setNull(j);
                } else {
                    deciVec.setBigInteger(j, trueValue);
                }
            }
            return deciVec;
        } else {
            for (int j = 0; j < vec.getSize(); j++) {
                if (vec.isNull(j)) {
                    continue;
                }
                Integer trueValue = getSignedIntValue(Integer.toUnsignedString(vec.get(j)));
                if (trueValue == null || trueValue < 0) {
                    vec.setNull(j);
                } else {
                    vec.set(j, trueValue);
                }
            }
            return vec;
        }
    }

    /**
     * convertUnsignedLong
     *
     * @param vec           unsigned long data is stored in LongVec
     * @param isDecimal     if table structure is decimal
     * @param decimalScale  pow of 10, which is used to obtain the true value of decimal
     * @return Vec          vector of the true data type
     */
    public Vec convertUnsignedLong(LongVec vec, boolean isDecimal, BigInteger decimalScale) {
        if (isDecimal) {
            Decimal128Vec deciVec = new Decimal128Vec(vec.getSize());
            for (int j = 0; j < vec.getSize(); j++) {
                if (vec.isNull(j) || getSignedLongValue(Long.toUnsignedString(vec.get(j))) == null) {
                    deciVec.setNull(j);
                    continue;
                }
                BigInteger trueValue = new BigInteger(getSignedLongValue(Long.toUnsignedString(vec.get(j)))
                        .toString());
                trueValue = trueValue.multiply(decimalScale);
                if (trueValue == null || trueValue.compareTo(BigInteger.ZERO) < 0) {
                    deciVec.setNull(j);
                } else {
                    deciVec.setBigInteger(j, trueValue);
                }
            }
            return deciVec;
        } else {
            for (int j = 0; j < vec.getSize(); j++) {
                if (vec.isNull(j)) {
                    continue;
                }
                Long trueValue = getSignedLongValue(Long.toUnsignedString(vec.get(j)));
                if (trueValue == null || trueValue < 0) {
                    vec.setNull(j);
                } else {
                    vec.set(j, trueValue);
                }
            }
            return vec;
        }
    }

    /**
     * processUnsignedData
     * resolve problems: 1. when reading unsignedInt/unsignedLong data from Parquet file
     * 2. when the real type stored in Parquet file is different with the type specified by talbe structure
     */
    public void processUnsignedData() {
        List<Type> types = this.fileSchema.getFields();
        TableScanDesc desc = (TableScanDesc) this.tableScanOp.getConf();
        ArrayList<ColumnInfo> infoList = this.tableScanOp.getSchema().getSignature();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            if (!(type instanceof PrimitiveType) || ((PrimitiveType) type).getOriginalType() == null
                    || !((PrimitiveType) type).getOriginalType().name().contains("UINT")) {
                continue;
            }
            int scale = 0;
            boolean isDecimal = false;
            ObjectInspector inspector = infoList.get(i).getObjectInspector();
            if (inspector.getTypeName().contains("decimal")) {
                DecimalTypeInfo info = (DecimalTypeInfo) ((PrimitiveObjectInspector) inspector).getTypeInfo();
                scale = info.getScale();
                isDecimal = true;
            }
            BigInteger decimalScale = BigInteger.TEN.pow(scale);
            if (vecs[i] instanceof IntVec) {
                IntVec vec = (IntVec) vecs[i];
                vecs[i] = convertUnsignedInt(vec, isDecimal, decimalScale);
            } else if (vecs[i] instanceof LongVec) {
                LongVec vec = (LongVec) vecs[i];
                vecs[i] = convertUnsignedLong(vec, isDecimal, decimalScale);
            }
        }
    }

    @Override
    public boolean next(NullWritable key, VecBatchWrapper value) throws IOException {
        int batchSize = BATCH;
        if (tableScanOp != null && tableScanOp.getDone()) {
            return false;
        }
        if (neededTypes == null) {
            List<PrimitiveTypeInfo> primitiveTypeInfos = ((OmniTableScanOperator) tableScanOp).getNeedTypes();
            neededTypes = primitiveTypeInfos.stream().map(neededType -> TypeUtils.buildInputDataType(neededType)).collect(Collectors.toList());
        }
        batchSize = recordReader.next(neededTypes, vecs, missingColumns);
        if (batchSize == 0) {
            return false;
        }
        processUnsignedData();
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