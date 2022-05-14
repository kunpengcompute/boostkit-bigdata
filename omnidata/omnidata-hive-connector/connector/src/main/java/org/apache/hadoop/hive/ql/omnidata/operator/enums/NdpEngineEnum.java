package org.apache.hadoop.hive.ql.omnidata.operator.enums;

/**
 * Hive engine: tez mr spark
 *
 * @since 2022-03-21
 */
public enum NdpEngineEnum {
    // supported
    Tez("tez"),
    MR("mr"),
    Spark("spark");

    private String engine;

    NdpEngineEnum(String engine) {
        this.engine = engine;
    }

    public String getEngine() {
        return engine;
    }
}
