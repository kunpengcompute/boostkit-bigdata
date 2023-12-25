package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class ConditionExpression extends BaseExpression {

    private Integer width;
    private Integer precision;
    private Integer scale;
    private BaseExpression condition;
    @SerializedName("if_true")
    private BaseExpression ifTrue;
    @SerializedName("if_false")
    private BaseExpression ifFalse;

    public ConditionExpression(Integer returnType, Integer width, Integer precision, Integer scale) {
        super("IF", returnType, null);
        this.width = width;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void add(BaseExpression node) {
        if (condition == null) {
            condition = node;
        } else if (ifTrue == null) {
            ifTrue = node;
        } else if (ifFalse == null) {
            ifFalse = node;
        } else {
            ifFalse.add(node);
        }
    }

    @Override
    public boolean isFull() {
        return condition != null && ifTrue != null && ifFalse != null;
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

    public ConditionExpression copyBase() {
        return new ConditionExpression(this.getReturnType(), this.width, this.precision, this.scale);
    }

}
