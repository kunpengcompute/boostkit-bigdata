package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

import javax.annotation.Nullable;

public class BetweenExpression extends BaseExpression{

    @Nullable
    private BaseExpression lower_bound;
    @Nullable
    private BaseExpression upper_bound;
    @Nullable
    private BaseExpression value;

    public BetweenExpression(String exprType, Integer returnType, String operator) {
        super(exprType, returnType, operator);
    }

    public BetweenExpression(String exprType, Integer returnType, String operator, BaseExpression lower_bound,
        BaseExpression upper_bound, BaseExpression value) {
        super(exprType, returnType, operator);
        this.lower_bound = lower_bound;
        this.upper_bound = upper_bound;
        this.value = value;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public void add(BaseExpression node) {

    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public void setLocated(Located located) {

    }

    @Override
    public Located getLocated() {
        return null;
    }
}
