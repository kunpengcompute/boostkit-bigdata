/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.exchange

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.adaptive.ock.common.OmniRuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.common.RuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.common._
import org.apache.spark.sql.execution.adaptive.ock.memory._

import java.util

class BoostTuningColumnarShuffleExchangeHelper(exchange: BoostTuningShuffleExchangeLike, sparkContext: SparkContext)
  extends BoostTuningShuffleExchangeHelper(exchange, sparkContext) {

  override val executionMem: Long = shuffleManager match {
    case OCKBoostShuffleDefine.OCK_SHUFFLE_MANAGER_DEFINE =>
      BoostShuffleExecutionModel().apply()
    case OmniOpDefine.COLUMNAR_SHUFFLE_MANAGER_DEFINE =>
      ColumnarExecutionModel().apply()
    case OmniOCKShuffleDefine.OCK_COLUMNAR_SHUFFLE_MANAGER_DEFINE =>
      ColumnarExecutionModel().apply()
    case _ => 
      OriginExecutionModel().apply()
  }

  override protected def fillInput(input: util.LinkedHashMap[String, String]): Unit = {
    input.put("executionSize", executionMem.toString)
    input.put("upstreamDataSize", exchange.getUpStreamDataSize.toString)
    input.put("partitionRatio", initPartitionRatio.toString)
    var spillThreshold = if (OMNI_SPILL_ROW_ENABLED) {
      Math.min(OMNI_SPILL_ROWS, numElementsForceSpillThreshold)
    } else {
      numElementsForceSpillThreshold 
    }
    if (spillThreshold == Integer.MAX_VALUE) {
      spillThreshold = -1
    }

    input.put("elementSpillThreshold", spillThreshold.toString)
  }
}