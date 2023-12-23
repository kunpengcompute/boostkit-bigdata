
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

@Explain(displayName = "Omni Select Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniSelectDesc extends SelectDesc {
    public OmniSelectDesc(SelectDesc selectDesc) {
        super(selectDesc.getColList(), selectDesc.getOutputColumnNames(), selectDesc.isSelStarNoCompute());
        this.setSelStarNoCompute(selectDesc.isSelStarNoCompute());
    }
}