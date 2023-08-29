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

package com.huawei.boostkit.omnituning;

import com.huawei.boostkit.omnituning.exception.OmniTuningException;
import com.huawei.boostkit.omnituning.executor.OmniTuningRunner;
import org.apache.commons.lang.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

public final class OmniTuning {
    private static final int REQUIRED_PARAMS_LENGTH = 2;
    private static final String[] TIME_PARSE_PATTERNS = {"yyyy-MM-dd HH:mm:ss"};

    private OmniTuning() {}

    public static void main(String[] args) {
        if (args.length != REQUIRED_PARAMS_LENGTH) {
            throw new OmniTuningException("The number of parameters is abnormal. Only two parameters are supported.");
        }

        Date startDate;
        Date finishDate;
        try {
            startDate = DateUtils.parseDate(args[0], TIME_PARSE_PATTERNS);
            finishDate = DateUtils.parseDate(args[1], TIME_PARSE_PATTERNS);
        } catch (ParseException e) {
            throw new OmniTuningException("Unsupported date format. Only the 'yyyy-MM-dd HH:mm:ss' is supported", e);
        }

        long startTimeMills = startDate.getTime();
        long finishedTimeMills = finishDate.getTime();

        if (startTimeMills > finishedTimeMills) {
            throw new OmniTuningException("start time cannot be greater than finish time");
        }

        OmniTuningContext.getInstance();
        OmniTuningRunner runner = new OmniTuningRunner(startTimeMills, finishedTimeMills);
        runner.run();
    }
}
