/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.memory

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningLogger._
import org.apache.spark.sql.execution.adaptive.ock.common.RuntimeConfiguration._

case class ColumnarExecutionModel() extends ExecutionModel {
  override def apply(): Long = {
    val systemMem = executorMemory
    val executorCores = SparkEnv.get.conf.get(config.EXECUTOR_CORES).toLong
    val reservedMem = SparkEnv.get.conf.getLong("spark.testing.reservedMemory", 300 * 1024 * 1024)
    val usableMem = systemMem - reservedMem
    val shuffleMemFraction = SparkEnv.get.conf.get(config.MEMORY_FRACTION) *
      (1 - SparkEnv.get.conf.get(config.MEMORY_STORAGE_FRACTION))
    val offHeapMem = if (offHeapEnabled) {
      offHeapSize
    } else {
      0
    }
    val finalMem = ((usableMem * shuffleMemFraction + offHeapMem) / executorCores).toLong
    TLogDebug(s"ExecutorMemory is $systemMem reserved $reservedMem offHeapMem is $offHeapMem" +
      s" shuffleMemFraction is $shuffleMemFraction, execution memory of executor is $finalMem")
    finalMem
  }
}