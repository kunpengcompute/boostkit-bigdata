package com.huawei.boostkit.hive.expression;

import java.util.List;

public class CastFunctionExpression extends FunctionExpression {
    public CastFunctionExpression(Integer returnType, Integer width, Integer precision, Integer scale) {
        super(returnType, "CAST", 1, width, precision, scale);
    }

    @Override
    public void add(BaseExpression node) {
        if (node instanceof CastFunctionExpression) {
            List<BaseExpression> arguments = ((CastFunctionExpression) node).getArguments();
            arguments.forEach(CastFunctionExpression.super::add);
            return;
        }
        super.add(node);
    }

    public Integer getDataType() {
        return ((ReferenceFactor)super.getArguments().get(0)).dataType;
    }

    @Override
    public Integer getReturnType() {
        return super.getReturnType();
    }
}
