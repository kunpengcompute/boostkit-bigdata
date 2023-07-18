/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef JNI_OCK_SHUFFLE_JNI_READER
#define JNI_OCK_SHUFFLE_JNI_READER

#include <jni.h>
/* Header for class com_huawei_ock_spark_jni_OckShuffleJniReader */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniReader
 * Method:    make
 * Signature: ([I)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_make(JNIEnv *, jobject, jintArray);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniReader
 * Method:    close
 * Signature: (JI)I
 */
JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_close(JNIEnv *, jobject, jlong);
/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniReader
 * Method:    nativeGetVectorBatch
 * Signature: (JJIII;Ljava/lang/Long;)I
 */
JNIEXPORT jint JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeGetVectorBatch(JNIEnv *, jobject,
    jlong, jlong, jint, jint, jint, jobject);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniReader
 * Method:    nativeGetVector
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeGetVecValueLength(JNIEnv *, jobject,
    jlong, jint);

/*
 * Class:     com_huawei_ock_spark_jni_OckShuffleJniReader
 * Method:    nativeCopyVecDataInVB
 * Signature: (JJI)V
 */
JNIEXPORT void JNICALL Java_com_huawei_ock_spark_jni_OckShuffleJniReader_nativeCopyVecDataInVB(JNIEnv *, jobject,
    jlong, jlong, jint);

#ifdef __cplusplus
}
#endif
#endif // JNI_OCK_SHUFFLE_JNI_READER