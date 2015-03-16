/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_voltdb_jni_ExecutionEngine */

#ifndef _Included_org_voltdb_jni_ExecutionEngine
#define _Included_org_voltdb_jni_ExecutionEngine
#ifdef __cplusplus
extern "C" {
#endif
#undef org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS
#define org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS 0L
#undef org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR
#define org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR 1L
#undef org_voltdb_jni_ExecutionEngine_ERRORCODE_WRONG_SERIALIZED_BYTES
#define org_voltdb_jni_ExecutionEngine_ERRORCODE_WRONG_SERIALIZED_BYTES 101L
#undef org_voltdb_jni_ExecutionEngine_ERRORCODE_NO_DATA
#define org_voltdb_jni_ExecutionEngine_ERRORCODE_NO_DATA 102L
/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nextDependencyTest
 * Signature: (I)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_voltdb_jni_ExecutionEngine_nextDependencyTest
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeCreate
 * Signature: (Z)J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeCreate
  (JNIEnv *, jobject, jboolean);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeDestroy
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDestroy
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeInitialize
 * Signature: (JIIIILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeInitialize
  (JNIEnv *, jobject, jlong, jint, jint, jint, jint, jstring);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetBuffers
 * Signature: (JLjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetBuffers
  (JNIEnv *, jobject, jlong, jobject, jint, jobject, jint, jobject, jint, jobject, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeLoadCatalog
 * Signature: (JLjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeLoadCatalog
  (JNIEnv *, jobject, jlong, jstring);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeUpdateCatalog
 * Signature: (JLjava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeUpdateCatalog
  (JNIEnv *, jobject, jlong, jstring, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeLoadTable
 * Signature: (JI[BJJJZ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeLoadTable
  (JNIEnv *, jobject, jlong, jint, jbyteArray, jlong, jlong, jlong, jboolean);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeExecutePlanFragment
 * Signature: (JJIIJJJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecutePlanFragment
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeExecuteCustomPlanFragment
 * Signature: (JLjava/lang/String;IIJJJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecuteCustomPlanFragment
  (JNIEnv *, jobject, jlong, jstring, jint, jint, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeExecuteQueryPlanFragmentsAndGetResults
 * Signature: (J[JI[I[IJJJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecuteQueryPlanFragmentsAndGetResults
  (JNIEnv *, jobject, jlong, jlongArray, jint, jintArray, jintArray, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSerializeTable
 * Signature: (JILjava/nio/ByteBuffer;I)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSerializeTable
  (JNIEnv *, jobject, jlong, jint, jobject, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTick
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTick
  (JNIEnv *, jobject, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeQuiesce
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeQuiesce
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetStats
 * Signature: (JI[IZJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetStats
  (JNIEnv *, jobject, jlong, jint, jintArray, jboolean, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeToggleProfiler
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeToggleProfiler
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeHashinate
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeHashinate
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetUndoToken
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetUndoToken
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeReleaseUndoToken
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeReleaseUndoToken
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeUndoUndoToken
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeUndoUndoToken
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetLogLevels
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetLogLevels
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeActivateTableStream
 * Signature: (JII)Z
 */
JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeActivateTableStream
  (JNIEnv *, jobject, jlong, jint, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTableStreamSerializeMore
 * Signature: (JJIIII)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTableStreamSerializeMore
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jint, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeProcessRecoveryMessage
 * Signature: (JJII)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeProcessRecoveryMessage
  (JNIEnv *, jobject, jlong, jlong, jint, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTableHashCode
 * Signature: (JI)J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTableHashCode
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeExportAction
 * Signature: (JZZZZJJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExportAction
  (JNIEnv *, jobject, jlong, jboolean, jboolean, jboolean, jboolean, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTrackingEnable
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTrackingEnable
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTrackingFinish
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTrackingFinish
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTrackingReadSet
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTrackingReadSet
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTrackingWriteSet
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTrackingWriteSet
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheInitialize
 * Signature: (JLjava/lang/String;JIJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheInitialize
  (JNIEnv *, jobject, jlong, jstring, jlong, jint, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheAddDB
 * Signature: (JLjava/lang/String;JIJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheAddDB
  (JNIEnv *, jobject, jlong, jstring, jlong, jint, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheReadBlocks
 * Signature: (JI[I[I)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheReadBlocks
  (JNIEnv *, jobject, jlong, jint, jintArray, jintArray);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheEvictBlock
 * Signature: (JIJI)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheEvictBlock
  (JNIEnv *, jobject, jlong, jint, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheEvictBlockInBatch
 * Signature: (JIIJI)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheEvictBlockInBatch
  (JNIEnv *, jobject, jlong, jint, jint, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeAntiCacheMergeBlocks
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeAntiCacheMergeBlocks
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetRSS
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetRSS
  (JNIEnv *, jclass);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeMMAPInitialize
 * Signature: (JLjava/lang/String;JJ)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeMMAPInitialize
  (JNIEnv *, jobject, jlong, jstring, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeARIESInitialize
 * Signature: (JLjava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeARIESInitialize
  (JNIEnv *, jobject, jlong, jstring, jstring);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeDoAriesRecoveryPhase
 * Signature: (JJJJ)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDoAriesRecoveryPhase
  (JNIEnv *, jobject, jlong, jlong, jlong, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetArieslogBufferLength
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetArieslogBufferLength
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeRewindArieslogBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeRewindArieslogBuffer
  (JNIEnv *, jobject, jlong);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeReadAriesLogForReplay
 * Signature: (J[J)J
 */
JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeReadAriesLogForReplay
  (JNIEnv *, jobject, jlong, jlongArray);

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeFreePointerToReplayLog
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeFreePointerToReplayLog
  (JNIEnv *, jobject, jlong, jlong);

#ifdef __cplusplus
}
#endif
#endif
