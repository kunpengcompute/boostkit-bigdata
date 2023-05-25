/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef THESTRAL_PLUGIN_MASTER_JNI_COMMON_H
#define THESTRAL_PLUGIN_MASTER_JNI_COMMON_H

#include <jni.h>
#include "common/common.h"

spark::CompressionKind GetCompressionType(JNIEnv* env, jstring codec_jstr);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig);

#define JNI_FUNC_START try {

#define JNI_FUNC_END(exceptionClass)                \
    }                                               \
    catch (const std::exception &e)                 \
    {                                               \
        env->ThrowNew(exceptionClass, e.what());    \
        return 0;                                   \
    }                                               \


#define JNI_FUNC_END_VOID(exceptionClass)           \
    }                                               \
    catch (const std::exception &e)                 \
    {                                               \
        env->ThrowNew(exceptionClass, e.what());    \
        return;                                     \
    }                                               \

extern jclass runtimeExceptionClass;
extern jclass splitResultClass;
extern jclass jsonClass;
extern jclass arrayListClass;
extern jclass threadClass;

extern jmethodID jsonMethodInt;
extern jmethodID jsonMethodLong;
extern jmethodID jsonMethodHas;
extern jmethodID jsonMethodString;
extern jmethodID jsonMethodJsonObj;
extern jmethodID arrayListGet;
extern jmethodID arrayListSize;
extern jmethodID jsonMethodObj;
extern jmethodID splitResultConstructor;
extern jmethodID currentThread;
extern jmethodID threadGetId;

#endif //THESTRAL_PLUGIN_MASTER_JNI_COMMON_H
