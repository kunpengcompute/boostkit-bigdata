
package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.expression.ExpressionUtils;

import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;

@Explain(displayName = "Omni Sort Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniSortDesc extends AbstractOperatorDesc {
    private ExprNodeColumnDesc[] sortCol;
    public OmniSortDesc(GroupByDesc groupByDesc) {
        sortCol = new ExprNodeColumnDesc[groupByDesc.getKeys().size()];
        for (int i = 0; i < groupByDesc.getKeys().size(); i++) {
            sortCol[i] = (ExprNodeColumnDesc) ExpressionUtils.getExprNodeColumnDesc(groupByDesc.getKeys().get(i))
                    .get(0);
        }
    }

    public ExprNodeColumnDesc[] getSortCol() {
        return sortCol;
    }
}