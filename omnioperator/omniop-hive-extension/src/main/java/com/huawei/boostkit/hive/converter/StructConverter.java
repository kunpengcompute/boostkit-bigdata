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

package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.List;

public class StructConverter implements VecConverter {
    private VecConverter[] converters;
    private StructField[] fields;

    List<? extends StructField> fieldsList;
    private transient PrimitiveTypeInfo[] typeInfos;

    public StructConverter(StructObjectInspector structObjectInspector) {
        fieldsList = structObjectInspector.getAllStructFieldRefs();
        fields = fieldsList.stream().map(field -> (StructField) field).toArray(StructField[]::new);
        converters = fieldsList.stream()
                .filter(field -> field.getFieldObjectInspector() instanceof PrimitiveObjectInspector)
                .map(field -> CONVERTER_MAP
                        .get(((PrimitiveObjectInspector) field.getFieldObjectInspector()).getPrimitiveCategory()))
                .toArray(VecConverter[]::new);
        typeInfos = fieldsList.stream()
                .map(field -> ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo())
                .toArray(PrimitiveTypeInfo[]::new);
    }

    /**
     * convert data from hive type to java type
     *
     * @param col                   hive data
     * @param structObjectInspector structObjectInspector
     * @return converted data
     */
    public Object calculateValue(Object col, StructObjectInspector structObjectInspector) {
        Object[] result = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = converters[i].calculateValue(structObjectInspector.getStructFieldData(col, fields[i]),
                    typeInfos[i]);
        }
        return result;
    }

    public VecConverter[] getConverters() {
        return converters;
    }

    public List<? extends StructField> getFields() {
        return fieldsList;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        throw new RuntimeException(String.format("%s doesn't support toOmniVecV2", this.getClass().getSimpleName()));
    }
}
