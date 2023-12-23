package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

public class DecimalLiteral extends LiteralFactor<Object> {
    private Integer precision;
    private Integer scale;

    public DecimalLiteral(Object value, Integer dataType, Integer precision, Integer scale) {
        super("LITERAL", null, null, value, null, dataType);
        this.precision = precision;
        this.scale = scale;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getScale() {
        return scale;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
