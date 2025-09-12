package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan;
// represent as json string in description


import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.TypeDescriptionPOJO;

import java.util.Arrays;
import java.util.List;


/**
 * CSVFormatPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class CSVFormatPOJO {
    private String format; //"csv"
    private String delimiter; // only support default comma
    private String filePath; // local file path
    private List<TypeDescriptionPOJO> fields; // csv field type
    private int[] selectFields;
    private int[] csvSelectFieldToProjectFieldMapping;
    private int[] csvSelectFieldToCsvFieldMapping;

    // Default constructor
    public CSVFormatPOJO() {
    }

    // Full argument constructor
    public CSVFormatPOJO(String format, String delimiter, String filePath, List<TypeDescriptionPOJO> fields, int[] selectFields, int[] csvSelectFieldToProjectFieldMapping, int[] csvSelectFieldToCsvFieldMapping) {
        this.format = format;
        this.delimiter = delimiter;
        this.filePath = filePath;
        this.fields = fields;
        this.selectFields = selectFields;
        this.csvSelectFieldToProjectFieldMapping = csvSelectFieldToProjectFieldMapping;
        this.csvSelectFieldToCsvFieldMapping = csvSelectFieldToCsvFieldMapping;
    }

    // Getters and setters
    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<TypeDescriptionPOJO> getFields() {
        return fields;
    }

    public void setFields(List<TypeDescriptionPOJO> fields) {
        this.fields = fields;
    }

    public int[] getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(int[] selectFields) {
        this.selectFields = selectFields;
    }

    public int[] getCsvSelectFieldToProjectFieldMapping() {
        return csvSelectFieldToProjectFieldMapping;
    }

    public void setCsvSelectFieldToProjectFieldMapping(int[] csvSelectFieldToProjectFieldMapping) {
        this.csvSelectFieldToProjectFieldMapping = csvSelectFieldToProjectFieldMapping;
    }

    public int[] getCsvSelectFieldToCsvFieldMapping() {
        return csvSelectFieldToCsvFieldMapping;
    }

    public void setCsvSelectFieldToCsvFieldMapping(int[] csvSelectFieldToCsvFieldMapping) {
        this.csvSelectFieldToCsvFieldMapping = csvSelectFieldToCsvFieldMapping;
    }


    @Override
    public String toString() {
        return "CSVFormatPOJO{"
                + "format='" + format + '\'' +
                ", delimiter='" + delimiter + '\''
                + ", filePath='" + filePath + '\'' +
                ", fields=" + fields
                + ", selectFields=" + Arrays.toString(selectFields) +
                ", csvSelectFieldToProjectFieldMapping=" + Arrays.toString(csvSelectFieldToProjectFieldMapping)
                + ", csvSelectFieldToCsvFieldMapping=" + Arrays.toString(csvSelectFieldToCsvFieldMapping) +
                '}';
    }
}
