package org.apache.hadoop.hive.ql.omnidata.operator.aggregation;


/**
 * Ndp Arithmetic Expression Info
 * <p>
 * This class obtains variables such as colId based on the Hive expression string and checks whether the value of colId is a number.
 *
 * @since 2022-01-22
 */
public class NdpArithmeticExpressionInfo {
    private boolean[] isVal;

    private int[] colId;

    private String[] colType;

    private String[] colValue;

    public NdpArithmeticExpressionInfo(String vectorExpressionParameters) {
        // vectorExpressionParameters : col 2:bigint, col 6:bigint
        String[] parameters = vectorExpressionParameters.split(", ");
        int length = parameters.length;
        isVal = new boolean[length];
        colId = new int[length];
        colType = new String[length];
        colValue = new String[length];
        for (int j = 0; j < length; j++) {
            if (parameters[j].startsWith("val ")) {
                isVal[j] = true;
                colValue[j] = parameters[j].substring("val ".length());
            } else {
                isVal[j] = false;
                colId[j] = Integer.parseInt(parameters[j].split(":")[0].substring("col ".length()));
                colType[j] = parameters[j].split(":")[1];
            }
        }
    }

    public boolean[] getIsVal() {
        return isVal;
    }

    public int[] getColId() {
        return colId;
    }

    public String[] getColType() {
        return colType;
    }

    public String[] getColValue() {
        return colValue;
    }

}