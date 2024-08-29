/*
 * CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
package com.huawei.bdsolution.loadsmetric.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {
    @ResponseBody
    @GetMapping("/hello")
    public String hello() {
        return "Hello SpringBoot!";
    }
}
