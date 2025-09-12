package org.apache.flink.client.cli;

import com.huawei.omniruntime.flink.client.NativeMain;

import java.util.Properties;

public class UdfConfig {

    private static final UdfConfig INSTANCE = new UdfConfig();
    private Properties config = new Properties();

    public static UdfConfig getINSTANCE() {
        return INSTANCE;
    }

    public void setJarNumber(String jarFilePath) {
        config = NativeMain.getConfig(jarFilePath);
        for (String stringPropertyName : config.stringPropertyNames()) {
            if (!config.getProperty(stringPropertyName).endsWith(".so")) {
                config.remove(stringPropertyName);
            }
        }
    }

    public Properties getConfig() {
        return config;
    }
}
