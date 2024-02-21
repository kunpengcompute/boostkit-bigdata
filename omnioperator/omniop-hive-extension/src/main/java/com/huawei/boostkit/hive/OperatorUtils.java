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
