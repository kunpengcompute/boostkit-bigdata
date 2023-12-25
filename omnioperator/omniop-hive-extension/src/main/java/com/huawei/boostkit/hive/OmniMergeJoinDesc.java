
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Explain(displayName = "Omni Merge Join Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniMergeJoinDesc extends JoinDesc {
    private Map<Byte, List<ExprNodeDesc>> keys;

    public OmniMergeJoinDesc(JoinDesc joinDesc) {
        super(joinDesc);
    }

    public OmniMergeJoinDesc(CommonMergeJoinDesc commonMergeJoinDesc) {
        this((JoinDesc) commonMergeJoinDesc);
        keys = commonMergeJoinDesc.getKeys();
    }

    @Override
    @Explain(displayName = "keys")
    public Map<String, String> getKeysString() {
        Map<String, String> keyMap = new LinkedHashMap<>();
        for (Map.Entry<Byte, List<ExprNodeDesc>> k : keys.entrySet()) {
            keyMap.put(String.valueOf(k.getKey()), PlanUtils.getExprListString(k.getValue()));
        }
        return keyMap;
    }

    @Override
    @Explain(displayName = "keys", explainLevels = {Explain.Level.USER})
    public Map<Byte, String> getUserLevelExplainKeysString() {
        Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
        for (Map.Entry<Byte, List<ExprNodeDesc>> k : keys.entrySet()) {
            keyMap.put(k.getKey(), PlanUtils.getExprListString(k.getValue(), true));
        }
        return keyMap;
    }
}
