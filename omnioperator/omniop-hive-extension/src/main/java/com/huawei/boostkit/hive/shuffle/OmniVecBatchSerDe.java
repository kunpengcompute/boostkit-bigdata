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

package com.huawei.boostkit.hive.shuffle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

public class OmniVecBatchSerDe extends AbstractSerDe {
    public static final Logger LOG = LoggerFactory.getLogger(OmniVecBatchSerDe.class.getName());
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    TypeInfo rowTypeInfo;
    ObjectInspector cachedObjectInspector;
    int serializedSize;
    int deserializedSize;
    SerDeStats stats;
    boolean lastOperationSerialize;
    boolean lastOperationDeserialize;
    BytesWritable serializeBytesWritable = new BytesWritable();
    private transient VecSerdeBody[] deserializeResult;

    private transient int[] columnTypeLen;

    private transient ColumnSerDe[] columnSerDes;

    public static final Map<PrimitiveObjectInspector.PrimitiveCategory, Integer> TYPE_LEN = new HashMap<PrimitiveObjectInspector.PrimitiveCategory, Integer>() {
        {
            put(PrimitiveObjectInspector.PrimitiveCategory.BYTE, 1);
            put(PrimitiveObjectInspector.PrimitiveCategory.SHORT, 2);
            put(PrimitiveObjectInspector.PrimitiveCategory.INT, 4);
            put(PrimitiveObjectInspector.PrimitiveCategory.LONG, 8);
            put(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN, 1);
            put(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, 8);
            put(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, 8);
            put(PrimitiveObjectInspector.PrimitiveCategory.DATE, 4);
            put(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, 16);
        }
    };

    public OmniVecBatchSerDe() throws SerDeException {
    }

    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties) throws SerDeException {
        String columnNameProperty = properties.getProperty("columns");
        String columnNameDelimiter = properties.containsKey("column.name.delimiter")
                ? properties.getProperty("column.name.delimiter")
                : String.valueOf(',');
        String columnTypeProperty = properties.getProperty("columns.types");
        if (columnNameProperty.length() == 0) {
            this.columnNames = new ArrayList();
        } else {
            this.columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
        }

        if (columnTypeProperty.length() == 0) {
            this.columnTypes = new ArrayList();
        } else {
            this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        assert this.columnNames.size() == this.columnTypes.size();
        this.rowTypeInfo = TypeInfoFactory.getStructTypeInfo(this.columnNames, this.columnTypes);
        this.cachedObjectInspector = LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(this.rowTypeInfo);
        LOG.debug("LazyBinarySerDe initialized with: columnNames=" + this.columnNames + " columnTypes="
                + this.columnTypes);
        this.serializedSize = 0;
        this.stats = new SerDeStats();
        this.lastOperationSerialize = false;
        this.lastOperationDeserialize = false;
        initialSerializeParam();
    }

    private void initialSerializeParam() {
        columnTypeLen = new int[columnTypes.size()];
        columnSerDes = new ColumnSerDe[columnTypes.size()];
        int writeLen = 0;
        for (int i = 0; i < columnTypes.size(); i++) {
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) columnTypes.get(i))
                    .getPrimitiveCategory();
            columnTypeLen[i] = TYPE_LEN.getOrDefault(primitiveCategory, 0);
            if (columnTypeLen[i] == 0) {
                writeLen = writeLen + getEstimateLen((PrimitiveTypeInfo) columnTypes.get(i)) + 4;
                columnSerDes[i] = new VariableWidthColumnSerDe();
            } else {
                writeLen = writeLen + columnTypeLen[i] + 1;
                columnSerDes[i] = new FixedWidthColumnSerDe(columnTypeLen[i]);
            }
        }
        deserializeResult = new VecSerdeBody[columnTypes.size()];
        for (int i = 0; i < deserializeResult.length; i++) {
            if (columnTypeLen[i] == 0) {
                deserializeResult[i] = new VecSerdeBody(getEstimateLen((PrimitiveTypeInfo) columnTypes.get(i)));
            } else {
                deserializeResult[i] = new VecSerdeBody(columnTypeLen[i]);
                deserializeResult[i].length = columnTypeLen[i];
            }
        }
        if (columnTypes.size() == 0) {
            this.serializeBytesWritable.set(new byte[0], 0, 0);
            this.serializedSize = 0;
            this.lastOperationSerialize = true;
            this.lastOperationDeserialize = false;
        } else {
            serializeBytesWritable.setCapacity(writeLen);
        }
    }

    public static int getEstimateLen(PrimitiveTypeInfo primitiveTypeInfo) {
        if (primitiveTypeInfo.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
            return 512;
        } else {
            // one chinese char uses 3 bytes in UTF8
            return ((BaseCharTypeInfo) primitiveTypeInfo).getLength() * 3;
        }
    }

    public Class<? extends Writable> getSerializedClass() {
        return BytesWritable.class;
    }

    private long[] fieldId;

    public void setFieldId(long[] fieldId) {
        this.fieldId = fieldId;
    }

    public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {
        if (fieldId.length == 0) {
            return this.serializeBytesWritable;
        }
        VecWrapper[] vecWrappers = (VecWrapper[]) obj;
        int totalLen = 0;
        byte[] writeBytes = this.serializeBytesWritable.getBytes();
        for (int i = 0; i < fieldId.length; ++i) {
            totalLen = columnSerDes[i].serialize(writeBytes, vecWrappers[(int) fieldId[i]], totalLen);
        }
        this.serializeBytesWritable.setSize(totalLen);
        this.serializedSize = totalLen;
        this.lastOperationSerialize = true;
        this.lastOperationDeserialize = false;
        return this.serializeBytesWritable;
    }

    public SerDeStats getSerDeStats() {
        assert this.lastOperationSerialize != this.lastOperationDeserialize;

        if (this.lastOperationSerialize) {
            this.stats.setRawDataSize(this.serializedSize);
        } else {
            this.stats.setRawDataSize(this.deserializedSize);
        }

        return this.stats;
    }

    public Object deserialize(Writable field) {
        BytesWritable b = (BytesWritable) field;
        this.deserializedSize = b.getLength();
        this.lastOperationSerialize = false;
        this.lastOperationDeserialize = true;
        int offset = 0;
        byte[] bytes = b.getBytes();
        for (int count = 0; count < deserializeResult.length; ++count) {
            offset = columnSerDes[count].deserialize(deserializeResult[count], bytes, offset);
        }
        return deserializeResult;
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.cachedObjectInspector;
    }

    public List<TypeInfo> getColumnTypes() {
        return columnTypes;
    }

    public int[] getColumnTypeLen() {
        return columnTypeLen;
    }
}
