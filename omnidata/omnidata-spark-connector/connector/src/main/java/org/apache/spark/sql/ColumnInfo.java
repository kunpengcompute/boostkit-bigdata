package org.apache.spark.sql;

import io.prestosql.spi.type.Type;

public class ColumnInfo {
    private PrestoExpressionInfo expressionInfo;

    private Type prestoType;

    private int filterProjectionId;

    public ColumnInfo(PrestoExpressionInfo expressionInfo, Type prestoType, int filterProjectionId) {
        this.expressionInfo = expressionInfo;
        this.prestoType = prestoType;
        this.filterProjectionId = filterProjectionId;
    }

    public PrestoExpressionInfo getExpressionInfo() {
        return expressionInfo;
    }

    public Type getPrestoType() {
        return prestoType;
    }

    public int getFilterProjectionId() {
        return filterProjectionId;
    }
}
