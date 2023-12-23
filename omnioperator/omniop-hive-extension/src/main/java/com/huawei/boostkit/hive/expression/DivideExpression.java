package com.huawei.boostkit.hive.expression;

import javax.annotation.Nullable;

public class DivideExpression extends CompareExpression {
    private Integer precision;
    private Integer scale;

    public DivideExpression(Integer returnType, String operator, Integer precision, Integer scale) {
        super("BINARY", returnType, operator);
        this.precision = precision;
        this.scale = scale;
    }

    public DivideExpression(Integer returnType, @Nullable BaseExpression left, @Nullable BaseExpression right) {
        super("BINARY", returnType, "DIVIDE", left, right);
    }
}
