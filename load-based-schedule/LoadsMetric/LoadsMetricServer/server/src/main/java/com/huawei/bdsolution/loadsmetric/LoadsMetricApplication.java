/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric;

import com.huawei.bdsolution.loadsmetric.entity.AppConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.validation.annotation.Validated;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@EnableScheduling
@Validated
@EnableConfigurationProperties(AppConfig.class)
public class LoadsMetricApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoadsMetricApplication.class, args);
    }

}
