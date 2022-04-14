/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata;

/**
 * Property for omniData communication
 *
 * @since 2021-08-27
 */
public class OmniDataProperty {
    private OmniDataProperty() {}

    /**
     * constant string for "grpc.client.target.list"
     */
    public static final String GRPC_CLIENT_TARGET_LIST = "grpc.client.target.list";

    /**
     * delimiter string for "grpc.client.target.list"
     * examples: 192.0.2.1:80,192.0.2.2:80
     */
    public static final String HOSTADDRESS_DELIMITER = ",";

    /**
     * constant string for "grpc.client.target"
     */
    public static final String GRPC_CLIENT_TARGET = "grpc.client.target";

    /**
     * constant string for "grpc.ssl.enabled"
     */
    public static final String GRPC_SSL_ENABLED = "grpc.ssl.enabled";

    /**
     * Directory of Public Key Infrastructure.
     */
    public static final String PKI_DIR = "pki.dir";

    /**
     * Path to the SSL client certificate file.
     */
    public static final String GRPC_CLIENT_CERT_PATH = "grpc.client.cert.file.path";

    /**
     * Path to the SSL private key file.
     */
    public static final String GRPC_CLIENT_PRIVATE_KEY_PATH = "grpc.client.private.key.file.path";

    /**
     * Path to the SSL trust certificate file.
     */
    public static final String GRPC_TRUST_CA_PATH = "grpc.trust.ca.file.path";

    /**
     * Path to the SSL Certificate Revocation List file.
     */
    public static final String GRPC_CRL_PATH = "grpc.crl.file.path";

}

