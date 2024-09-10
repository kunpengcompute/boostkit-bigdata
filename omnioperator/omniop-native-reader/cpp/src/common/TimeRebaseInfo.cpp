/**
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

#include <parquet/exception.h>
#include "TimeRebaseInfo.h"
#include "jni/jni_common.h"

namespace common {
    int64_t TimeRebaseInfo::timestampRebaseFunc(int64_t micros) 
    {
        if (timestampRebaseMode == MODE_CORRECTED || timestampRebasePtr == nullptr) {
            return micros;
        }
        switch (timestampRebaseMode) {
            case MODE_EXCEPTION: {
                if (micros < lastSwitchJulianTs) {
                   throw parquet::ParquetStatusException(::arrow::Status::RError(
                        R"(The files may be written by Spark 2.x or legacy versions of Hive, reading timestamps before 1900-01-01T00:00:00Z from Parquet files can be ambiguous. You can set the SQL config "spark.sql.parquet.datetimeRebaseModeInRead" or the datasource option "datetimeRebaseMode" to "LEGACY" to rebase the datetime values w.r.t. the calendar difference during reading. To read the datetime values as it is, set the SQL config "spark.sql.parquet.datetimeRebaseModeInRead" or the datasource option "datetimeRebaseMode" to 'CORRECTED'.)"));
                }
                return micros;
            }
            case MODE_LEGACY:
                return timestampRebasePtr->RebaseJulianToGregorianMicros(micros);
            default:
                throw parquet::ParquetStatusException(::arrow::Status::Invalid("Invalid timestampRebaseMode"));
        }
    }

    int64_t TimeRebaseInfo::int96RebaseFunc(int64_t micros) 
    {
        if (int96RebaseMode == MODE_CORRECTED || int96RebasePtr == nullptr) {
            return micros;
        }
        switch (int96RebaseMode) {
            case MODE_EXCEPTION: {
                if (micros < lastSwitchJulianTs) {
                    throw parquet::ParquetStatusException(::arrow::Status::RError(
                        R"(The files may be written by Spark 2.x or legacy versions of Hive, reading timestamps before 1900-01-01T00:00:00Z from Parquet INT96 files can be ambiguous. You can set the SQL config "spark.sql.parquet.int96RebaseModeInRead" or the datasource option "int96RebaseMode" to "LEGACY" to rebase the datetime values w.r.t. the calendar difference during reading. To read the datetime values as it is, set the SQL config "spark.sql.parquet.int96RebaseModeInRead" or the datasource option "int96RebaseMode" to 'CORRECTED'.)"));
                }
                return micros;
            }
            case MODE_LEGACY:
                return int96RebasePtr->RebaseJulianToGregorianMicros(micros);
            default:
                throw parquet::ParquetStatusException(::arrow::Status::Invalid("Invalid int96RebaseMode"));
        }
    }

    std::unique_ptr<TimeRebaseInfo> BuildTimeRebaseInfo(JNIEnv *env, jobject jsonObj) 
    {
        std::unique_ptr<JulianGregorianRebase> timestampRebasePtr;
        LegacyBehaviorPolicy timestampRebaseMode = MODE_INVALID;
        jboolean hasTimestampRebase = env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("timestampRebase"));
        if (hasTimestampRebase) {
            jobject timestampRebase =
                env->CallObjectMethod(jsonObj, jsonMethodJsonObj, env->NewStringUTF("timestampRebase"));
            timestampRebasePtr = BuildJulianGregorianRebase(env, timestampRebase);
            timestampRebaseMode = static_cast<LegacyBehaviorPolicy>(
                env->CallIntMethod(timestampRebase, jsonMethodInt, env->NewStringUTF("mode")));
        }

        std::unique_ptr<JulianGregorianRebase> int96RebasePtr;
        LegacyBehaviorPolicy int96RebaseMode = MODE_INVALID;
        jboolean hasInt96Rebase = env->CallBooleanMethod(jsonObj, jsonMethodHas, env->NewStringUTF("int96Rebase"));
        if (hasInt96Rebase) {
            jobject int96Rebase = env->CallObjectMethod(jsonObj, jsonMethodJsonObj, env->NewStringUTF("int96Rebase"));
            int96RebasePtr = BuildJulianGregorianRebase(env, int96Rebase);
            int96RebaseMode = static_cast<LegacyBehaviorPolicy>(
                env->CallIntMethod(int96Rebase, jsonMethodInt, env->NewStringUTF("mode")));
        }

        int64_t lastSwitchJulianTs = std::numeric_limits<int64_t>::min();
        if (hasTimestampRebase || hasInt96Rebase) {
            lastSwitchJulianTs = env->CallIntMethod(jsonObj, jsonMethodLong, env->NewStringUTF("lastSwitchJulianTs"));
        }

        return std::make_unique<TimeRebaseInfo>(timestampRebasePtr, timestampRebaseMode, int96RebasePtr,
            int96RebaseMode, lastSwitchJulianTs);
    }
}
