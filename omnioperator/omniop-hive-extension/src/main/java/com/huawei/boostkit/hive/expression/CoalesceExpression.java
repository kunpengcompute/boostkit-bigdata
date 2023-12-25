package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

public class CoalesceExpression extends BaseExpression {
    private Integer width;
    private Integer precision;
    private Integer scale;
    private BaseExpression value1;
    private BaseExpression value2;

    public CoalesceExpression(Integer returnType, Integer width, Integer precision, Integer scale) {
        super("COALESCE", returnType, null);
        this.width = width;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void add(BaseExpression node) {
        if (value1 == null) {
            value1 = node;
            return;
        }
        if (value2 == null) {
            value2 = node;
        }
    }

    @Override
    public boolean isFull() {
        return value1.isFull() && value2.isFull();
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
}
