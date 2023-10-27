/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.configuration;

import org.apache.commons.configuration2.PropertiesConfiguration;

public class OmniAdvisorConfigure {
    private static final int DEFAULT_THREAD_COUNT = 3;
    private static final String THREAD_COUNT_CONF_KEY = "log.analyzer.thread.count";
    private static final String KERBEROS_PRINCIPAL_KEY = "kerberos.principal";
    private static final String KERBEROS_KEYTAB_FILE_KEY = "kerberos.keytab.file";

    private final int threadCount;
    private String kerberosPrincipal;
    private String kerberosKeytabFile;

    public OmniAdvisorConfigure(PropertiesConfiguration configuration) {
        this.threadCount = configuration.getInt(THREAD_COUNT_CONF_KEY, DEFAULT_THREAD_COUNT);
        this.kerberosPrincipal = configuration.getString(KERBEROS_PRINCIPAL_KEY, null);
        this.kerberosKeytabFile = configuration.getString(KERBEROS_KEYTAB_FILE_KEY, null);
    }

    public int getThreadCount() {
        return threadCount;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public String getKerberosKeytabFile() {
        return kerberosKeytabFile;
    }

    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    public void setKerberosKeytabFile(String kerberosKeytabFile) {
        this.kerberosKeytabFile = kerberosKeytabFile;
    }
}
