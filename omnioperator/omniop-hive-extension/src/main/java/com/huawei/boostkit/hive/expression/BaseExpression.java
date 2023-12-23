package com.huawei.boostkit.hive.expression;

import java.util.List;

public abstract class BaseExpression {
    public enum Located {
        ROOT,
        LEFT,
        RIGHT
    }

    private String exprType;
    private Integer returnType;
    private String operator;
    private transient int level = 0;
    protected transient BaseExpression parent;

    public BaseExpression(String exprType, Integer returnType, String operator) {
        this.exprType = exprType;
        this.returnType = returnType;
        this.operator = operator;
    }

    public String getExprType() {
        return exprType;
    }

    public Integer getReturnType() {
        return returnType;
    }

    public String getOperator() {
        return operator;
    }

    public void setParent(BaseExpression parent) {
        this.parent = parent;
    }

    public BaseExpression getParent() {
        return parent;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public abstract void add(BaseExpression node);

    public abstract boolean isFull();

    public abstract void setLocated(Located located);

    public abstract Located getLocated();
}