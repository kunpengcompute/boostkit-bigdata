/**
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef OMNI_RUNTIME_JNI_COMMON_H
#define OMNI_RUNTIME_JNI_COMMON_H

#include <jni.h>

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

#define JNI_FUNC_END_WITH_VECBATCH(exceptionClass, recordBatch)  \
    } catch (const std::exception &e) {                          \
        for (auto vec : recordBatch) {                           \
            delete vec;                                          \
        }                                                        \
        recordBatch.clear();                                     \
        env->ThrowNew(runtimeExceptionClass, e.what());          \
        return 0;                                                \
    }                                                            \

extern jclass runtimeExceptionClass;
extern jclass jsonClass;
extern jclass arrayListClass;
extern jclass threadClass;

extern jmethodID jsonMethodInt;
extern jmethodID jsonMethodLong;
extern jmethodID jsonMethodHas;
extern jmethodID jsonMethodString;
extern jmethodID jsonMethodJsonObj;
extern jmethodID arrayListGet;
extern jmethodID arrayListAdd;
extern jmethodID arrayListSize;
extern jmethodID jsonMethodObj;
extern jmethodID currentThread;

#endif //OMNI_RUNTIME_JNI_COMMON_H
