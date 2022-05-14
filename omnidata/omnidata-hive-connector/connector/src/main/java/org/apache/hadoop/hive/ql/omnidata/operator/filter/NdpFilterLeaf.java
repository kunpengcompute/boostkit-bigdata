package org.apache.hadoop.hive.ql.omnidata.operator.filter;

import org.apache.hadoop.hive.ql.omnidata.operator.filter.NdpFilter.*;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Ndp Filter Leaf
 * <p>
 * Supported ExprNodeDesc:
 * 1.ExprNodeConstantDesc
 * 2.ExprNodeColumnDesc
 * 3.ExprNodeGenericFuncDesc
 * <p>
 * Example: SELECT * FROM table1,table2 where table1.a = table2.b;
 * ndpLeafOperator : =
 * leftFilterFunc : table1.a
 * rightFilterFunc : table2.b
 *
 * @since 2021-11-23
 */
public class NdpFilterLeaf {
    private static final Logger LOG = LoggerFactory.getLogger(NdpFilterLeaf.class);

    private NdpLeafOperator ndpLeafOperator;

    private List<Object> constantList = new ArrayList<>();

    private NdpFilterFunc leftFilterFunc;

    private NdpFilterFunc rightFilterFunc;

    public NdpFilterLeaf(NdpLeafOperator ndpLeafOperator) {
        this.ndpLeafOperator = ndpLeafOperator;
    }

    public void parseExprLeaf(ExprNodeGenericFuncDesc exprLeaf) {
        if (ndpLeafOperator.equals(NdpLeafOperator.UNSUPPORTED)) {
            return;
        }
        int argumentIndex = 0;
        if (ndpLeafOperator.equals(NdpLeafOperator.BETWEEN)) {
            // first argument for BETWEEN should be boolean type
            argumentIndex = 1;
        }
        ExprNodeDesc exprNodeDesc = exprLeaf.getChildren().get(argumentIndex);
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            setLeftFilterFunc((ExprNodeColumnDesc) exprNodeDesc);
        } else if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            // exists Udf
            parseLeftUdfExpr((ExprNodeGenericFuncDesc) exprNodeDesc);
        } else {
            LOG.info("OmniData Hive Filter [{}] failed to push down, since unsupported this class: [{}]",
                this.ndpLeafOperator.name(), exprNodeDesc.getClass());
            this.ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
            return;
        }
        switch (ndpLeafOperator) {
            case IN:
                parseInExprValue(exprLeaf);
                break;
            case BETWEEN:
                parseBetweenExprValue(exprLeaf);
                break;
            case EQUAL:
            case GREATER_THAN:
            case LESS_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN_OR_EQUAL:
            case LIKE:
                parseCompareExprValue(exprLeaf);
                break;
        }
    }

    public NdpLeafOperator getNdpLeafOperator() {
        return ndpLeafOperator;
    }

    public NdpFilterFunc getLeftFilterFunc() {
        return leftFilterFunc;
    }

    public void setLeftFilterFunc(String returnType, ExprNodeColumnDesc columnDesc, ExprNodeDesc udfExpression) {
        leftFilterFunc = new NdpFilterFunc(returnType, columnDesc.getColumn(),
            columnDesc.getIsPartitionColOrVirtualCol(), udfExpression);
    }

    public void setLeftFilterFunc(ExprNodeColumnDesc columnDesc) {
        leftFilterFunc = new NdpFilterFunc(columnDesc.getTypeString(), columnDesc.getColumn(),
            columnDesc.getIsPartitionColOrVirtualCol(), null);
    }

    public NdpFilterFunc getRightFilterFunc() {
        return rightFilterFunc;
    }

    public void setRightFilterFunc(String returnType, ExprNodeColumnDesc columnDesc, ExprNodeDesc udfExpression) {
        rightFilterFunc = new NdpFilterFunc(returnType, columnDesc.getColumn(),
            columnDesc.getIsPartitionColOrVirtualCol(), udfExpression);
    }

    public void setRightFilterFunc(ExprNodeColumnDesc columnDesc) {
        rightFilterFunc = new NdpFilterFunc(columnDesc.getTypeString(), columnDesc.getColumn(),
            columnDesc.getIsPartitionColOrVirtualCol(), null);
    }

    public List<Object> getConstantList() {
        return constantList;
    }

    private void parseUdfExpr(ExprNodeGenericFuncDesc udfExpr, boolean isLeftFilterFunc) {
        ExprNodeDesc exprNodeDesc = udfExpr;
        while (exprNodeDesc.getChildren() != null) {
            exprNodeDesc = exprNodeDesc.getChildren().get(0);
        }
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            String udfReturnType = udfExpr.getTypeString();
            if (isLeftFilterFunc) {
                setLeftFilterFunc(udfReturnType, (ExprNodeColumnDesc) exprNodeDesc, udfExpr);
            } else {
                setRightFilterFunc(udfReturnType, (ExprNodeColumnDesc) exprNodeDesc, udfExpr);
            }
        } else {
            LOG.info("OmniData Hive Filter [{}] failed to push down, since unsupported this class: [{}]",
                this.ndpLeafOperator.name(), exprNodeDesc.getClass());
            this.ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
        }
    }

    private void parseLeftUdfExpr(ExprNodeGenericFuncDesc udfExpr) {
        parseUdfExpr(udfExpr, true);
    }

    private void parseRightUdfExpr(ExprNodeGenericFuncDesc udfExpr) {
        parseUdfExpr(udfExpr, false);
    }

    /**
     * name = "in"
     * _FUNC_(val1, val2...) - returns true if test equals any valN
     */
    private void parseInExprValue(ExprNodeGenericFuncDesc inExpr) {
        // start at index 1, since at 0 is the variable from table column
        for (ExprNodeDesc expr : inExpr.getChildren().subList(1, inExpr.getChildren().size())) {
            if (expr instanceof ExprNodeConstantDesc) {
                constantList.add(((ExprNodeConstantDesc) expr).getValue());
            } else {
                LOG.info("OmniData Hive Filter [IN] failed to push down, since unsupported this class: [{}]",
                    expr.getClass());
                this.ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
                return;
            }
        }
    }

    /**
     * name = "between"
     * _FUNC_ a [NOT] BETWEEN b AND c - evaluate if a is [not] in between b and c
     */
    private void parseBetweenExprValue(ExprNodeGenericFuncDesc betweenExpr) {
        if (betweenExpr.getChildren().get(2) instanceof ExprNodeConstantDesc && betweenExpr.getChildren()
            .get(3) instanceof ExprNodeConstantDesc) {
            ExprNodeConstantDesc leftValueInfo = (ExprNodeConstantDesc) betweenExpr.getChildren().get(2);
            ExprNodeConstantDesc rightValueInfo = (ExprNodeConstantDesc) betweenExpr.getChildren().get(3);
            constantList.add(leftValueInfo.getValue());
            constantList.add(rightValueInfo.getValue());
        } else {
            LOG.info("OmniData Hive Filter [BETWEEN] failed to push down, since unsupported this class: [{}] or [{}]",
                betweenExpr.getChildren().get(2).getClass(), betweenExpr.getChildren().get(3).getClass());
            this.ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
        }
    }

    private void parseCompareExprValue(ExprNodeGenericFuncDesc compareExpr) {
        ExprNodeDesc exprNodeDesc = compareExpr.getChildren().get(1);
        if (exprNodeDesc instanceof ExprNodeConstantDesc) {
            // On the right is a constant expression
            constantList.add(((ExprNodeConstantDesc) exprNodeDesc).getValue());
        } else if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            // On the right is a column
            setRightFilterFunc((ExprNodeColumnDesc) exprNodeDesc);
        } else if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            // On the right is a UDF expression
            parseRightUdfExpr((ExprNodeGenericFuncDesc) exprNodeDesc);
        } else {
            LOG.info("OmniData Hive Filter [{}] failed to push down, since unsupported this class: [{}]",
                this.ndpLeafOperator.name(), exprNodeDesc.getClass());
            this.ndpLeafOperator = NdpLeafOperator.UNSUPPORTED;
        }
    }

    public static class NdpFilterFunc {
        /**
         * if Func exists UDF, returnType is UDF's return type
         */
        private String returnType;

        private String columnName;

        private Boolean isPartitionColumn;

        private ExprNodeDesc udfExpression;

        public NdpFilterFunc(String returnType, String columnName, Boolean isPartitionColumn,
            ExprNodeDesc udfExpression) {
            this.returnType = returnType;
            this.columnName = columnName;
            this.isPartitionColumn = isPartitionColumn;
            this.udfExpression = udfExpression;
        }

        public String getReturnType() {
            return returnType;
        }

        public void setReturnType(String returnType) {
            this.returnType = returnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Boolean getPartitionColumn() {
            return isPartitionColumn;
        }

        public void setPartitionColumn(Boolean partitionColumn) {
            isPartitionColumn = partitionColumn;
        }

        public ExprNodeDesc getUdfExpression() {
            return udfExpression;
        }

        public void setUdfExpression(ExprNodeDesc udfExpression) {
            this.udfExpression = udfExpression;
        }

        public Boolean isExistsUdf() {
            return udfExpression != null;
        }

    }

}
