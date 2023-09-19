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

package com.huawei.boostkit.omnituning.security;

import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;

public final class HadoopSecurity {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurity.class);

    private String keytabLocation;
    private String keytabUser;
    private UserGroupInformation loginUser;

    public HadoopSecurity(Configuration conf) throws IOException {
        UserGroupInformation.setConfiguration(conf);
        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
        if (securityEnabled) {
            LOG.info("This cluster is Kerberos enabled.");
            boolean login = true;

            keytabUser = System.getProperty("keytab.user");
            if (keytabUser == null) {
                LOG.error("Keytab user not set. Please set keytab_user in the configuration file");
                login = false;
            }

            keytabLocation = System.getProperty("keytab.location");
            if (keytabLocation == null) {
                LOG.error("Keytab location not set. Please set keytab_location in the configuration file");
                login = false;
            }

            if (keytabLocation != null && !new File(keytabLocation).exists()) {
                LOG.error("The keytab file at location [" + keytabLocation + "] does not exist.");
                login = false;
            }

            if (!login) {
                throw new OmniTuningException("Cannot login. This cluster is security enabled.");
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
        LOG.info("Logging with " + keytabUser + " and " + keytabLocation);
        UserGroupInformation.loginUserFromKeytab(keytabUser, keytabLocation);
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
