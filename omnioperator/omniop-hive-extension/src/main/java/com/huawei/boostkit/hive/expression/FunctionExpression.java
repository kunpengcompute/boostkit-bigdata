package com.huawei.boostkit.hive.expression;

import com.google.gson.annotations.SerializedName;

public class FunctionExpression extends UnaryExpression {
    @SerializedName("function_name")
    private String functionName;

    private Integer precision;

    private Integer scale;

    private Integer width;

    public FunctionExpression(Integer returnType, String functionName, int size, Integer width) {
        super("FUNCTION", returnType, size);
        this.functionName = functionName;
        this.width = width;
    }

    public FunctionExpression(Integer returnType, String functionName, int size, Integer width, Integer precision, Integer scale) {
        this(returnType, functionName, size, width);
        this.precision = precision;
        this.scale = scale;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }
}
