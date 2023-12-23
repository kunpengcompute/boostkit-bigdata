package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
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

    public StringVecConverter(StructObjectInspector structObjectInspector) {
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
     * @param col hive data
     * @param structObjectInspector structObjectInspector
     * @return converted data
     */
    public Object calculateValue(Object col, StructObjectInspector structObjectInspector) {
        Object[] result = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = converters[i].calculateValue(getStructFieldData(col, fields[i], structObjectInspector), typeInfos[i]);
        }
    }

    public VecConverter[] getConverters() {
        return converters;
    }

    public List<? extends StructField> getFields() {
        return fieldsList;
    }

    public Object getStructFieldData(Object data, StructField fieldRef, StructObjectInspector structObjectInspector) {
        if (data == null) {
            return null;
        }
        if (!(structObjectInspector instanceof LazyBinaryStructObjectInspector)) {
            return structObjectInspector.getStructFieldData(data, fieldRef);
        }
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        throw new RuntimeException(String.format("%s doesn't support toOmniVecV2", this.getClass().getSimpleName()));
    }
}
