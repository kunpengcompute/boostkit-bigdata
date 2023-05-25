/**
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP
#define THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP

#include "jni_common.h"
#include "io/SparkFile.hh"
#include "SparkJniWrapper.hh"

jclass runtimeExceptionClass;
jclass splitResultClass;
jclass jsonClass;
jclass arrayListClass;
jclass threadClass;

jmethodID jsonMethodInt;
jmethodID jsonMethodLong;
jmethodID jsonMethodHas;
jmethodID jsonMethodString;
jmethodID jsonMethodJsonObj;
jmethodID arrayListGet;
jmethodID arrayListSize;
jmethodID jsonMethodObj;
jmethodID splitResultConstructor;
jmethodID currentThread;
jmethodID threadGetId;

static jint JNI_VERSION = JNI_VERSION_1_8;

spark::CompressionKind GetCompressionType(JNIEnv* env, jstring codec_jstr)
{
    auto codec_c = env->GetStringUTFChars(codec_jstr, JNI_FALSE);
    auto codec = std::string(codec_c);
    auto compression_type = GetCompressionType(codec);
    env->ReleaseStringUTFChars(codec_jstr, codec_c);
    return compression_type;
}

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name)
{
    jclass local_class = env->FindClass(class_name);
    jclass global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig)
{
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    return ret;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved)
{
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    runtimeExceptionClass = CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

    splitResultClass =
        CreateGlobalClassReference(env, "Lcom/huawei/boostkit/spark/vectorized/SplitResult;");
    splitResultConstructor = GetMethodID(env, splitResultClass, "<init>", "(JJJJJ[J)V");

    jsonClass = CreateGlobalClassReference(env, "org/json/JSONObject");
    jsonMethodInt = env->GetMethodID(jsonClass, "getInt", "(Ljava/lang/String;)I");
    jsonMethodLong = env->GetMethodID(jsonClass, "getLong", "(Ljava/lang/String;)J");
    jsonMethodHas = env->GetMethodID(jsonClass, "has", "(Ljava/lang/String;)Z");
    jsonMethodString = env->GetMethodID(jsonClass, "getString", "(Ljava/lang/String;)Ljava/lang/String;");
    jsonMethodJsonObj = env->GetMethodID(jsonClass, "getJSONObject", "(Ljava/lang/String;)Lorg/json/JSONObject;");
    jsonMethodObj = env->GetMethodID(jsonClass, "get", "(Ljava/lang/String;)Ljava/lang/Object;");

    arrayListClass = CreateGlobalClassReference(env, "java/util/ArrayList");
    arrayListGet = env->GetMethodID(arrayListClass, "get", "(I)Ljava/lang/Object;");
    arrayListSize = env->GetMethodID(arrayListClass, "size", "()I");

    threadClass = CreateGlobalClassReference(env, "java/lang/Thread");
    currentThread = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
    threadGetId = env->GetMethodID(threadClass, "getId", "()J");

    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved)
{
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    env->DeleteGlobalRef(runtimeExceptionClass);
    env->DeleteGlobalRef(splitResultClass);
    env->DeleteGlobalRef(jsonClass);
    env->DeleteGlobalRef(arrayListClass);
    env->DeleteGlobalRef(threadClass);

    g_shuffleSplitterHolder.Clear();
}

#endif //THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP
