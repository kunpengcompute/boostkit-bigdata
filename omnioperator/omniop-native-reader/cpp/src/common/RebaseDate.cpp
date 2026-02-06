/**
* Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "RebaseDate.h"
#include <algorithm>

namespace common {
const int32_t gregJulianDiffs[] = {-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

const int32_t gregJulianDiffSwitchDay[] = {
        -719162, -682944, -646420, -609896, -536847, -500323, -463799, -390750,
        -354226, -317702, -244653, -208129, -171605, -141436, -141435, -141434,
        -141433, -141432, -141431, -141430, -141429, -141428, -141427};

const int32_t length = 23;

const int32_t gregorianCommonEraStartDay = gregJulianDiffSwitchDay[0];

const int32_t gregJulian100YearDay = 36524;

const int32_t gregJulian200YearDay = 73049;

int32_t localRebaseGregorianToJulianDays(int32_t days) {
    int32_t diff = -2;
    int32_t switchDay = gregJulianDiffSwitchDay[1];
    int32_t offset = switchDay - days - gregJulian200YearDay;
    if (offset <= 0) {
        return days + diff;
    }
    int32_t step = offset / (2 * gregJulian100YearDay + gregJulian200YearDay);
    diff += (-3) * step;
    offset = offset % (2 * gregJulian100YearDay + gregJulian200YearDay);
    if (offset == 0) {
        return days + diff;
    }
    step = offset / gregJulian100YearDay;
    diff += (-1) * std::min(step, 3);
    offset = offset % gregJulian100YearDay;
    diff += (offset == 0 || step == 3) ? 0 : - 1;
    return days + diff;
}

int32_t rebaseDays(int32_t days) {
    int32_t i = length;
    do {
        i -= 1;
    } while (i > 0 && days < gregJulianDiffSwitchDay[i]);
    return days + gregJulianDiffs[i];
}

int32_t rebaseGregorianToJulianDays(int32_t days) {
    if (days < gregorianCommonEraStartDay) {
        return localRebaseGregorianToJulianDays(days);
    } else {
        return rebaseDays(days);
    }
}
}