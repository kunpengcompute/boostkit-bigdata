package com.huawei.boostkit.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

import java.util.HashMap;

class OmniHiveConf {
    public boolean enableOperatorJoin;
    public boolean enableOperatorMapJoin;
    public boolean enableOperatorMergejoin;
    public boolean enableOperatorGroupby;
    public boolean enableOperatorSelect;
    public boolean enableOperatorFilter;
    public boolean enableOperatorPtf;
    public boolean enableOperatorTablescan;
    public boolean enableOperatorReducesink;
    private HashMap<OperatorType, Boolean> operatorEnableMap;

    public OmniHiveConf(HiveConf hiveConf) {
        enableOperatorJoin = hiveConf.getBoolean("omni.hive.join.enabled", true);
        enableOperatorMapJoin = hiveConf.getBoolean("omni.hive.mapjoin.enabled", true);
        enableOperatorMergejoin = hiveConf.getBoolean("omni.hive.mergejoin.enabled", true);
        enableOperatorGroupby = hiveConf.getBoolean("omni.hive.groupby.enabled", true);
        enableOperatorSelect = hiveConf.getBoolean("omni.hive.select.enabled", true);
        enableOperatorFilter = hiveConf.getBoolean("omni.hive.filter.enabled", true);
        enableOperatorPtf = hiveConf.getBoolean("omni.hive.ptf.enabled", true);
        enableOperatorTablescan = hiveConf.getBoolean("omni.hive.tablescan.enabled", true);
        enableOperatorReducesink = hiveConf.getBoolean("omni.hive.reducesink.enabled", true);
        operatorEnableMap = new HashMap<OperatorType, Boolean>() {{
            put(OperatorType.JOIN, enableOperatorJoin);
            put(OperatorType.MAPJOIN, enableOperatorMapJoin);
            put(OperatorType.MERGEJOIN, enableOperatorMergejoin);
            put(OperatorType.GROUPBY, enableOperatorGroupby);
            put(OperatorType.SELECT, enableOperatorSelect);
            put(OperatorType.FILTER, enableOperatorFilter);
            put(OperatorType.PTF, enableOperatorPtf);
            put(OperatorType.TABLESCAN, enableOperatorTablescan);
            put(OperatorType.REDUCESINK, enableOperatorReducesink);
        }};
    }

    public boolean isEnableOperator(OperatorType operatorType) {
        return operatorEnableMap.get(operatorType);
    }
}