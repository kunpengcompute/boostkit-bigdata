package org.apache.spark.sql;

/**
 * 构造Page传输参数
 */
public class PageCandidate {

    public String filePath;

    public Long startPos;

    public Long splitLen;

    public int columnOffset;

    public String sdiHosts;

    private String fileFormat;

    public int maxFailedTimes;

    public PageCandidate(String filePath, Long startPos, Long splitLen, int columnOffset,
                         String sdiHosts, String fileFormat, int maxFailedTimes) {
        this.filePath = filePath;
        this.startPos = startPos;
        this.splitLen = splitLen;
        this.columnOffset = columnOffset;
        this.sdiHosts = sdiHosts;
        this.fileFormat = fileFormat;
        this.maxFailedTimes = maxFailedTimes;
    }

    public Long getStartPos() {
        return startPos;
    }

    public Long getSplitLen() {
        return splitLen;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getColumnOffset() {
        return columnOffset;
    }

    public String getSdiHosts() {
        return sdiHosts;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public int getMaxFailedTimes() {
        return maxFailedTimes;
    }
}
