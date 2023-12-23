package com.huawei.boostkit.hive.expression;

import javax.annotation.Nullable;

public class ReferenceFactor extends BaseExpression {
    protected Integer colVal;
    @Nullable
    private final Integer width;
    protected Integer dataType;

    public ReferenceFactor(String exprType, Integer returnType,
                           String operator, Integer colVal, @Nullable Integer width, Integer dataType) {
        super(exprType, returnType, operator);
        this.colVal = colVal;
        this.width = width;
        this.dataType = dataType;
    }

    @Override
    public void add(BaseExpression node) {

    }

    @Override
    public boolean isFull() {
        return true;
    }

    @Override
    public void setLocated(Located located) {

    }

    @Override
    public Located getLocated() {
        return null;
    }

    @Override
    public String toString() {
        if (width == null) {
            return String.format("{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d}", dataType, colVal);
        }
        return String.format(
                "{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d, " + "\"width\":%d}",
                dataType, colVal, width);
    }
}
