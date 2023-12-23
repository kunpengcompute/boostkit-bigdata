
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * OmniVectorDesc.
 */
@Explain(displayName = "Omni Vector Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniVectorDesc extends AbstractOperatorDesc {
    private static final long serialVersionUID = 1L;
    private boolean isToVector;

    public OmniVectorDesc(boolean isToVector) {
        this.isToVector = isToVector;
    }

    public boolean getIsToVector() {
        return this.isToVector;
    }

    @Explain(displayName = "Convert Type", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
            Explain.Level.EXTENDED})
    public String getConvertType() {
        if (isToVector) {
            return "From row to omni vector";
        }
        return "From omni vector to row";
    }

    @Override
    public boolean isSame(OperatorDesc other) {
        if (getClass().getName().equals(other.getClass().getName())) {
            OmniVectorDesc otherDesc -= (OmniVectorDesc) other;
            return getIsToVector() == otherDesc.getIsToVector();
        }
        return false;
    }
}