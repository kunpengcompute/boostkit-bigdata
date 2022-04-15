package org.apache.spark.sql;

import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * PrestoExpressionInfo
 *
 * @date 2021/6/18 15:49
 */
public class PrestoExpressionInfo {

    private int projectionId;

    private Type returnType;

    private Type fieldDataType;

    private Expression childExpression;

    private RowExpression prestoRowExpression;

    public PrestoExpressionInfo() {
    }

    public int getProjectionId() {
        return projectionId;
    }

    public void setProjectionId(int projectionId) {
        this.projectionId = projectionId;
    }

    public RowExpression getPrestoRowExpression() {
        return prestoRowExpression;
    }

    public void setPrestoRowExpression(RowExpression prestoRowExpression) {
        this.prestoRowExpression = prestoRowExpression;
    }

    public Type getReturnType() {
        return returnType;
    }

    public void setReturnType(Type returnType) {
        this.returnType = returnType;
    }

    public Type getFieldDataType() {
        return fieldDataType;
    }

    public void setFieldDataType(Type fieldDataType) {
        this.fieldDataType = fieldDataType;
    }

    public Expression getChildExpression() {
        return childExpression;
    }

    public void setChildExpression(Expression childExpression) {
        this.childExpression = childExpression;
    }
}
