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

package com.huawei.boostkit.omnituning.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolListener extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolListener.class);

    private final Timer timer;
    private final ThreadPoolExecutor executor;

    public ThreadPoolListener(Timer timer, ThreadPoolExecutor executor) {
        this.timer = timer;
        this.executor = executor;
    }

    @Override
    public void run() {
        LOG.info("Executor taskCount{}, active count {}, complete count {}",
                executor.getTaskCount(), executor.getActiveCount(), executor.getCompletedTaskCount());
        if (executor.getActiveCount() == 0) {
            executor.shutdown();
            timer.cancel();
        }
    }
}
