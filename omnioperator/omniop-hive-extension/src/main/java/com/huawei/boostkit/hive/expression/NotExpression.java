package com.huawei.boostkit.hive.expression;

import nove.hetu.omniruntime.type.BooleanDataType;

public class NotExpression extends UnaryExpression {
    private BaseExpression expr;

    public NotExpression() {
        super("UNARY", BooleanDataType.BOOLEAN.getId().toValue(), 0, "not");
    }

    public void setExpr(BaseExpression expr) {
        this.expr = expr;
    }
}
