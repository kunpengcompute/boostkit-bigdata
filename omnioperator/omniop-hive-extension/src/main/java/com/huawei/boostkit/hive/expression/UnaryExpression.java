package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

public class UnaryExpression extends BaseExpression {

    private List<BaseExpression> arguments;

    private final transient int size;

    public UnaryExpression(String exprType, Integer returnType, int size) {
        super(exprType, returnType ,null);
        this.size = size;
        this.arguments = new ArrayList<>(size);
    }

    public UnaryExpression(String exprType, Integer returnType, int size, String operator) {
        super(exprType, returnType ,operator);
        this.size = size;
        this.arguments = new ArrayList<>(size);
    }

    @Override
    public void add(BaseExpression node){
        if (isFull()) {
            return;
        }
        arguments.add(node);
    }

    public List<BaseExpression> getArguments() {
        return arguments;
    }

    @Override
    public boolean isFull() {
        return arguments.size() == size;
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