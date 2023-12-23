package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;

import javax.annotations.Nullable;
import java.util.HashSet;

public class CompareExpression extends BaseExpression {

    @Nullable
    private BaseExpression left;
    @Nullable
    private BaseExpression right;

    private transient Located located = Located.ROOT;

    private static final HashSet<Class<? extends BaseExpression>> specialNode =
            new HashSet<Class<? extends BaseExpression>>() {{
                add(CompareExpression.class);
                add(FunctionExpression.class);
                add(NotExpression.class);
                add(ConditionExpression.class);
            }};

    public CompareExpression(String exprType, Integer returnType, String operator) {
        super(exprType, returnType, operator);
        this.parent = this;
    }

    public CompareExpression(String exprType, Integer returnType, String operator,
                             @Nullable BaseExpression left, @Nullable BaseExpression right) {
        super(exprType, returnType, operator);
        this.left = left;
        this.right = right;
    }

    @Override
    public void add(BaseExpression node) {
        if (node == null) {
            return;
        }

        if (specialNode.contains(node.getClass()) && isFlatFull() && this.getLevel() == node.getLevel() - 1) {
            CompareExpression copyNode = this.copyBase();
            copyNode.add(this.getRight());
            copyNode.add(node);
            this.right = copyNode;
            copyNode.setParent(this);
            copyNode.setLocated(Located.RIGHT);
            return;
        }
        if (left == null) {
            left = node;
            left.setParent(this);
            left.setLocated(Located.LEFT);
        } else if (right == null) {
            right = node;
            right.setParent(this);
            right.setLocated(Located.RIGHT);
        } else if (!left.isFull()) {
            left.add(node);
            left.setParent(this);
            left.setLocated(Located.LEFT);
        } else {
            right.add(node);
            right.setParent(this);
            right.setLocated(Located.RIGHT);
        }
    }

    @Override
    public boolean isFull() {
        return left != null && right != null && left.isFull() && right.isFull();
    }

    private boolean isFlatFull() {
        return left != null && right != null;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Nullable
    public BaseExpression getRight() {
        return right;
    }

    @Nullable
    public BaseExpression getLeft() {
        return left;
    }

    @Override
    public void setLocated(Located located) {
        this.located = located;
    }

    @Override
    public Located getLocated() {
        return located;
    }

    public CompareExpression copyBase() {
        return new CompareExpression(this.getExprType(), this.getReturnType(), this.getOperator());
    }

}
