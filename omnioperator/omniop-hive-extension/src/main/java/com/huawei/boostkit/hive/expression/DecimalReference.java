package com.huawei.boostkit.hive.expression;

public class DecimalReference extends ReferenceFactor {
    private final Integer precision;

    private final Integer scale;

    public DecimalReference(Integer colVal, Integer dataType, Integer precision, Integer scale) {
        super("FIELD_REFERENCE", null, null, colVal, null, dataType);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d," + "\"precesion\":%d,"
                        + "\"scale\":%d}",
                dataType, colVal, precision, scale);
    }
}
