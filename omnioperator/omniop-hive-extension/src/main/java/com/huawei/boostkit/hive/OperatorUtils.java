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

package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OperatorUtils {
    public static ObjectInspector expandInspector(ObjectInspector inspector) {
        List<StructField> fields = new ArrayList<>();
        List<? extends StructField> keyValueFields = ((StructObjectInspector) inspector).getAllStructFieldRefs();
        List<? extends StructField> keyFields = ((StructObjectInspector) keyValueFields.get(0)
                .getFieldObjectInspector()).getAllStructFieldRefs();
        List<? extends StructField> valueFields = ((StructObjectInspector) keyValueFields.get(1)
                .getFieldObjectInspector()).getAllStructFieldRefs();
        List<String> fieldNames = keyFields.stream().map(field -> "key." + field.getFieldName())
                .collect(Collectors.toList());
        fieldNames.addAll(
                valueFields.stream().map(field -> "value." + field.getFieldName()).collect(Collectors.toList()));
        fields.addAll(keyFields);
        fields.addAll(valueFields);
        List<ObjectInspector> fieldInspectors = fields.stream().map(StructField::getFieldObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }
}
