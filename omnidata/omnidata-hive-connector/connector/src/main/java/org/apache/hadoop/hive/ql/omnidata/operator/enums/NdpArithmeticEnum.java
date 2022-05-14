package org.apache.hadoop.hive.ql.omnidata.operator.enums;

/**
 * Ndp NdpOperator Type Enum
 *
 * @since 2022-01-27
 */
public enum NdpArithmeticEnum {
    // *
    MULTIPLY,
    // +
    ADD,
    // -
    SUBTRACT,
    // /
    DIVIDE,
    // %
    MODULUS,
    CAST,
    CAST_IDENTITY,
    UNSUPPORTED;

    public static NdpArithmeticEnum getArithmeticByClass(Class arithmeticClass) {
        String arithmeticClassName = arithmeticClass.getSimpleName();
        if (arithmeticClassName.contains("Multiply")) {
            return MULTIPLY;
        } else if (arithmeticClassName.contains("Add")) {
            return ADD;
        } else if (arithmeticClassName.contains("Subtract")) {
            return SUBTRACT;
        } else if (arithmeticClassName.contains("Divide")) {
            return DIVIDE;
        } else if (arithmeticClassName.contains("Modulo")) {
            return MODULUS;
        } else if (arithmeticClassName.contains("IdentityExpression")) {
            return CAST_IDENTITY;
        } else if (arithmeticClassName.contains("Cast")) {
            return CAST;
        } else {
            return UNSUPPORTED;
        }
    }
}
