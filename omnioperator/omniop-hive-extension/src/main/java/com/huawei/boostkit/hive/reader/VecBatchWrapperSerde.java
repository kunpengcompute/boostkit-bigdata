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

import static com.huawei.boostkit.hive.reader.VecBatchWrapperSerde.COMPRESSION;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, COMPRESSION})
public class VecBatchWrapperSerde extends AbstractSerDe {
    public static final String COMPRESSION = "";

    private static final Logger LOG = LoggerFactory.getLogger(VecBatchWrapperSerde.class);

    private ObjectInspector inspector;

    @Override
    public void initialize(Configuration conf, Properties table) {
        String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
        // NOTE: if "columns.types" is missing, all columns will be of String type
        String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        final String columnNameDelimiter = table.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
                ? table.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
                : String.valueOf(SerDeUtils.COMMA);
        // Parse the configuration parameters
        ArrayList<String> columnNames = new ArrayList<String>();
        if (columnNameProperty != null && columnNameProperty.length() > 0) {
            for (String name : columnNameProperty.split(columnNameDelimiter)) {
                columnNames.add(name);
            }
        }
        if (columnTypeProperty == null) {
            // Default type: all string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) {
                    sb.append(":");
                }
                sb.append("string");
            }
            columnTypeProperty = sb.toString();
        }

        ArrayList<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        fieldTypes.forEach(fieldType -> {
            PrimitiveObjectInspector primitiveObjectInspector;
            if (fieldType instanceof PrimitiveTypeInfo) {
                primitiveObjectInspector = PrimitiveObjectInspectorFactory
                        .getPrimitiveJavaObjectInspector((PrimitiveTypeInfo) fieldType);
            } else {
                primitiveObjectInspector = PrimitiveObjectInspectorFactory
                        .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.VOID);
            }
            structFieldObjectInspectors.add(primitiveObjectInspector);
        });
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, structFieldObjectInspectors);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return VecBatchWrapper.class;
    }

    @Override
    public Writable serialize(Object realRow, ObjectInspector inspector) {
        throw new UnsupportedOperationException("can't serialize");
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return writable;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
