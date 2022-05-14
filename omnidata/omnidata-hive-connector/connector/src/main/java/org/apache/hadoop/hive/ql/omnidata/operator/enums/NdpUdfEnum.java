package org.apache.hadoop.hive.ql.omnidata.operator.enums;

import org.apache.hadoop.hive.ql.omnidata.physical.NdpPlanChecker;
import org.apache.hadoop.hive.ql.udf.UDFReplace;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;

/**
 * Hive Udf Enum
 */
public enum NdpUdfEnum {
    // Supported push-down Udf
    CAST("cast", "$operator$cast"),
    INSTR("instr", "instr"),
    LENGTH("length", "length"),
    LOWER("lower", "lower"),
    REPLACE("replace", "replace"),
    // unsupported split()
    SPLIT("split", "split"),
    SUBSCRIPT("SUBSCRIPT", "$operator$subscript"),
    SUBSTR("substr", "substr"),
    SUBSTRING("substring", "substring"),
    UPPER("upper", "upper"),
    UNSUPPORTED("", "");

    private String signatureName;

    private String operatorName;

    NdpUdfEnum(String signatureName, String operatorName) {
        this.signatureName = signatureName;
        this.operatorName = operatorName;
    }

    public static NdpUdfEnum getUdfType(GenericUDF genericUDF) {
        NdpUdfEnum resUdf = UNSUPPORTED;
        Class udfClass = (genericUDF instanceof GenericUDFBridge)
                ? ((GenericUDFBridge) genericUDF).getUdfClass()
                : genericUDF.getClass();
        if (isOpCast(udfClass)) {
            resUdf = CAST;
        } else if (udfClass == GenericUDFInstr.class) {
            resUdf = INSTR;
        } else if (udfClass == GenericUDFLength.class) {
            resUdf = LENGTH;
        } else if (udfClass == GenericUDFLower.class) {
            resUdf = LOWER;
        } else if (udfClass == UDFReplace.class) {
            resUdf = REPLACE;
        } else if (udfClass == GenericUDFIndex.class) {
            resUdf = SUBSCRIPT;
        } else if (udfClass == UDFSubstr.class) {
            resUdf = SUBSTR;
        } else if (udfClass == GenericUDFUpper.class) {
            resUdf = UPPER;
        }
        // udf needs white list verification
        return NdpPlanChecker.checkUdfByWhiteList(resUdf) ? resUdf : UNSUPPORTED;
    }

    public static boolean checkUdfSupported(GenericUDF genericUDF) {
        return !getUdfType(genericUDF).equals(UNSUPPORTED);
    }

    /**
     * need to support :
     * GenericUDFTimestamp.class GenericUDFToBinary.class GenericUDFToDecimal.class GenericUDFToTimestampLocalTZ.class
     *
     * @param udfClass Class
     * @return true or false
     */
    public static boolean isOpCast(Class udfClass) {
        return udfClass == UDFToBoolean.class || udfClass == UDFToByte.class || udfClass == UDFToDouble.class
                || udfClass == UDFToFloat.class || udfClass == UDFToInteger.class || udfClass == UDFToLong.class
                || udfClass == UDFToShort.class || udfClass == UDFToString.class || udfClass == GenericUDFToVarchar.class
                || udfClass == GenericUDFToChar.class || udfClass == GenericUDFToDate.class;
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
