package org.apache.spark.sql;

/**
 * udf enum
 */
public enum NdpUdfEnum {
    // Supported push-down Udf
    SUBSTRING("substr","substr"),
    LENGTH("length","length"),
    UPPER("upper","upper"),
    LOWER("lower","lower"),
    CAST("cast","$operator$cast"),
    REPLACE("replace","replace"),
    INSTR("instr","instr"),
    SUBSCRIPT("SUBSCRIPT","$operator$subscript"),
    SPLIT("split","split"),
    STRINGINSTR("instr","instr");

    private String signatureName;
    private String operatorName;

    NdpUdfEnum(String signatureName, String operatorName) {
        this.signatureName = signatureName;
        this.operatorName = operatorName;
    }

    public String getSignatureName() {
        return signatureName;
    }

    public void setSignatureName(String signatureName) {
        this.signatureName = signatureName;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }
}
