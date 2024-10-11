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

#ifndef TIMEREBASEINFO_H
#define TIMEREBASEINFO_H

#include <memory>
#include "JulianGregorianRebase.h"

namespace common {
    enum LegacyBehaviorPolicy {
        MODE_EXCEPTION = 0,
        MODE_LEGACY,
        MODE_CORRECTED,
        MODE_INVALID
    };

    class TimeRebaseInfo {
    public:
        TimeRebaseInfo(std::unique_ptr<JulianGregorianRebase> &tPtr, const LegacyBehaviorPolicy tMode,
            std::unique_ptr<JulianGregorianRebase> &iPtr, const LegacyBehaviorPolicy iMode, const int64_t lastTs)
            : timestampRebasePtr(std::move(tPtr)),
              timestampRebaseMode(tMode),
              int96RebasePtr(std::move(iPtr)),
              int96RebaseMode(iMode),
              lastSwitchJulianTs(lastTs)
        {}

        int64_t timestampRebaseFunc(int64_t micros);

        int64_t int96RebaseFunc(int64_t micros);

    private:
        std::unique_ptr<JulianGregorianRebase> timestampRebasePtr;
        LegacyBehaviorPolicy timestampRebaseMode;
        std::unique_ptr<JulianGregorianRebase> int96RebasePtr;
        LegacyBehaviorPolicy int96RebaseMode;
        int64_t lastSwitchJulianTs;
    };

    std::unique_ptr<TimeRebaseInfo> BuildTimeRebaseInfo(JNIEnv *env, jobject jsonObj);
}

#endif //TIMEREBASEINFO_H
