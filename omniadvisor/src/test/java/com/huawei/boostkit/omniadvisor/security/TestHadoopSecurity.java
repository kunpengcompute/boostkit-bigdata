/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omniadvisor.security;

import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Locale;
import java.util.Properties;

public class TestHadoopSecurity {
    private static final String KEYTAB_USER = "keytab.user";
    private static final String KEYTAB_LOCATION = "keytab.location";

    private static Configuration conf;
    private static MiniKdc kdc;
    private static File keytab;

    @BeforeClass
    public static void setupKdc() throws Exception {
        conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase(Locale.ENGLISH));
        UserGroupInformation.setConfiguration(conf);

        final String principal = "test";
        final File workDir = new File(System.getProperty("test.dir", "target"));
        keytab = new File(workDir, "test.keytab");

        Properties kdcConf = MiniKdc.createConf();
        kdc = new MiniKdc(kdcConf, workDir);
        kdc.start();
        kdc.createPrincipal(keytab, principal);
    }

    @AfterClass
    public static void tearDown() {
        UserGroupInformation.reset();
        if (kdc != null) {
            kdc.stop();
        }
    }

    @After
    public void clearProperties() {
        System.clearProperty(KEYTAB_USER);
        System.clearProperty(KEYTAB_LOCATION);
    }

    @Test
    public void testHadoopSecurity() throws Exception {
        System.setProperty(KEYTAB_USER, "test");
        System.setProperty(KEYTAB_LOCATION, keytab.getAbsolutePath());
        HadoopSecurity security = new HadoopSecurity(conf);
        security.checkLogin();
    }

    @Test(expected = OmniAdvisorException.class)
    public void testHadoopSecurityWithoutKeytabUser() throws Exception {
        System.setProperty(KEYTAB_LOCATION, keytab.getAbsolutePath());
        HadoopSecurity security = new HadoopSecurity(conf);
        security.checkLogin();
    }

    @Test(expected = OmniAdvisorException.class)
    public void testHadoopSecurityWithoutKeytabLocation() throws Exception {
        System.setProperty(KEYTAB_USER, "test");
        HadoopSecurity security = new HadoopSecurity(conf);
        security.checkLogin();
    }

    @Test(expected = OmniAdvisorException.class)
    public void testHadoopSecurityWithErrorKeytabFile() throws Exception {
        System.setProperty(KEYTAB_USER, "test");
        System.setProperty(KEYTAB_LOCATION, "errorPath");
        HadoopSecurity security = new HadoopSecurity(conf);
        security.checkLogin();

    }
}
