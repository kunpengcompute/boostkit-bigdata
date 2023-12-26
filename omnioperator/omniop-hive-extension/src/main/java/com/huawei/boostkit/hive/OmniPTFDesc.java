
package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.PTFDesc;

@Explain(displayName = "Omni PTF Operator", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT,
        Explain.Level.EXTENDED})
public class OmniPTFDesc extends PTFDesc {
    public OmniPTFDesc(PTFDesc desc) {
        setFuncDef(desc.getFuncDef());
        setLlInfo(desc.getLlInfo());
        setMapSide(desc.isMapSide());
        setCfg(desc.getCfg());
    }
}
