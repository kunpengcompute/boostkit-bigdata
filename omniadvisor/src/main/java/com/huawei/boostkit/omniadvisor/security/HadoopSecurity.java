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

package com.huawei.boostkit.omniadvisor.security;

import com.huawei.boostkit.omniadvisor.OmniAdvisorContext;
import com.huawei.boostkit.omniadvisor.configuration.OmniAdvisorConfigure;
import com.huawei.boostkit.omniadvisor.exception.OmniAdvisorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;

public final class HadoopSecurity {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurity.class);

    private String keytabFile;
    private String principal;
    private UserGroupInformation loginUser;

    public HadoopSecurity(Configuration hadoopConf) throws IOException {
        OmniAdvisorConfigure configure = OmniAdvisorContext.getInstance().getOmniAdvisorConfigure();
        UserGroupInformation.setConfiguration(hadoopConf);
        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
        if (securityEnabled) {
            LOG.info("This cluster is Kerberos enabled.");
            boolean login = true;

            principal = configure.getKerberosPrincipal();
            if (principal == null) {
                LOG.error("Keytab user not set. Please set keytab_user in the configuration file");
                login = false;
            }

            keytabFile = configure.getKerberosKeytabFile();
            if (keytabFile == null) {
                LOG.error("Keytab location not set. Please set keytab_location in the configuration file");
                login = false;
            }

            if (keytabFile != null && !new File(keytabFile).exists()) {
                LOG.error("The keytab file at location [" + keytabFile + "] does not exist.");
                login = false;
            }

            if (!login) {
                throw new OmniAdvisorException("Cannot login. This cluster is security enabled.");
            }
        }

        this.loginUser = getLoginUser();
    }

    public UserGroupInformation getUGI() throws IOException {
        checkLogin();
        return loginUser;
    }

    public UserGroupInformation getLoginUser() throws IOException {
        LOG.info("No login user. Creating login user");
        LOG.info("Logging with " + principal + " and " + keytabFile);
        UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        UserGroupInformation user = UserGroupInformation.getLoginUser();
        LOG.info("Logged in with user " + user);
        if (UserGroupInformation.isLoginKeytabBased()) {
            LOG.info("Login is keytab based");
        } else {
            LOG.info("Login is not keytab based");
        }
        return user;
    }

    public void checkLogin() throws IOException {
        if (loginUser == null) {
            loginUser = getLoginUser();
        } else {
            loginUser.checkTGTAndReloginFromKeytab();
        }
    }

    public <T> void doAs(PrivilegedAction<T> action) throws IOException {
        UserGroupInformation ugi = getUGI();
        if (ugi != null) {
            ugi.doAs(action);
        }
    }
}
