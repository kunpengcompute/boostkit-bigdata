package org.apache.spark.sql;

/**
 * OmniDataProperties
 *
 * @date 2021/8/6 16:15
 */
public class OmniDataProperties {

    private String grpcSslEnabled;

    private String grpcCertPath;

    private String grpcKeyPath;

    private String grpcCaPath;

    private String pkiDir;

    public OmniDataProperties(String grpcSslEnabled, String grpcCertPath,
                              String grpcKeyPath, String grpcCaPath, String pkiDir) {
        this.grpcSslEnabled = grpcSslEnabled;
        this.grpcCertPath = grpcCertPath;
        this.grpcKeyPath = grpcKeyPath;
        this.grpcCaPath = grpcCaPath;
        this.pkiDir = pkiDir;
    }

    public String isGrpcSslEnabled() {
        return grpcSslEnabled;
    }

    public String getGrpcCertPath() {
        return grpcCertPath;
    }

    public String getGrpcKeyPath() {
        return grpcKeyPath;
    }

    public String getGrpcCaPath() {
        return grpcCaPath;
    }

    public String getPkiDir() {
        return pkiDir;
    }
}

