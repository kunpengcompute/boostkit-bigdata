package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

import javax.annotation.Nullable;

public class LiteralFactor<T> extends BaseExpression {
    private Boolean isNull;

    private T value;

    @Nullable
    private Integer width;

    private Integer dataType;

    public LiteralFactor(String exprType, Integer returnType, String operator,
                         T value, @Nullable Integer width, Integer dataType) {
        super(exprType, returnType, operator);
        this.isNull = value == null;
        this.value = value;
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
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public Integer getDataType() {
        return dataType;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
