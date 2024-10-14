/**
 * Copyright (C) 2024-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "JulianGregorianRebase.h"
#include "jni/jni_common.h"

namespace common {
    static constexpr int64_t ONE_DAY_MICROS = 86400000000L;
    static constexpr int64_t JULIAN_100YEAR_MICROS= 3155760000000000L;

    JulianGregorianRebase::JulianGregorianRebase(std::string tz, const std::vector<int64_t> &switches,
        const std::vector<int64_t> &diffs) : tz(std::move(tz)), switches(switches), diffs(diffs)
    {
        offset = diffs[0] - ONE_DAY_MICROS * 2L;
        lastSwitch = switches[switches.size() - 1];
    }

    int64_t JulianGregorianRebase::RebaseJulianToGregorianMicros(int64_t micros)
    {
        if (micros >= lastSwitch) {
            return micros;
        }
        if (micros < switches[0]) {
            return micros + (ONE_DAY_MICROS * CalculateJulianDayOffset(micros)) + offset;
        }
        uint64_t i = switches.size() - 1;
        while (i > 0 && micros < switches[i]) {
            i -= 1;
        }
        return micros + diffs[i];
    }

    int64_t JulianGregorianRebase::CalculateJulianDayOffset(int64_t micros)
    {
        int64_t dayOffset = 2;
        int64_t switchMicros = switches[1];
        int64_t microsOffset = switchMicros - micros - JULIAN_100YEAR_MICROS;
        if (microsOffset <= 0) {
            return dayOffset;
        }
        int64_t step = microsOffset / JULIAN_100YEAR_MICROS - (microsOffset % JULIAN_100YEAR_MICROS == 0 ? 1 : 0);
        dayOffset += step - (step / 4);
        return dayOffset;
    }

    std::string JulianGregorianRebase::GetTz()
    {
        return tz;
    }

    std::unique_ptr<JulianGregorianRebase> BuildJulianGregorianRebase(JNIEnv *env, jobject jsonObj)
    {
        if (!env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("tz"))) {
            return nullptr;
        }

        auto jTz = static_cast<jstring>(env->CallObjectMethod(jsonObj, jsonMethodString, env->NewStringUTF("tz")));
        auto jSwitches = static_cast<jlongArray>(env->CallObjectMethod(jsonObj, jsonMethodObj, env->NewStringUTF("switches")));
        auto jDiffs = static_cast<jlongArray>(env->CallObjectMethod(jsonObj, jsonMethodObj, env->NewStringUTF("diffs")));

        auto tzPtr = env->GetStringUTFChars(jTz, JNI_FALSE);
        std::string tz(tzPtr);

        jlong* switchesPtr = env->GetLongArrayElements(jSwitches, JNI_FALSE);
        jsize switchesSize = env->GetArrayLength(jSwitches);
        std::vector<int64_t> switches(switchesPtr, switchesPtr + switchesSize);

        jlong* diffsPtr = env->GetLongArrayElements(jDiffs, JNI_FALSE);
        jsize diffsSize = env->GetArrayLength(jDiffs);
        std::vector<int64_t> diffs(diffsPtr, diffsPtr + diffsSize);

        env->ReleaseStringUTFChars(jTz, tzPtr);
        env->ReleaseLongArrayElements(jSwitches, switchesPtr, 0);
        env->ReleaseLongArrayElements(jDiffs, diffsPtr, 0);

        return std::make_unique<JulianGregorianRebase>(tz, switches, diffs);
    }
}
