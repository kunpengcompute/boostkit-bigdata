/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.exchange.estimator

import org.apache.spark.sql.execution.adaptive.ock.common.OmniRuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.common.RuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.exchange._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

case class ColumnarElementsForceSpillPartitionEstimator() extends PartitionEstimator {

  override def estimatorType: EstimatorType = ElementNumBased

  override def apply(exchange: ShuffleExchangeLike): Option[Int] = {
    if (!OMNI_SPILL_ROW_ENABLED && numElementsForceSpillThreshold == Integer.MAX_VALUE) {
      return None
    }

    val spillMinThreshold = if (OMNI_SPILL_ROW_ENABLED) {
      Math.min(OMNI_SPILL_ROWS, numElementsForceSpillThreshold)
    } else {
      numElementsForceSpillThreshold
    }

    exchange match {
      case ex: BoostTuningColumnarShuffleExchangeExec =>
        val rowCount = ex.inputColumnarRDD.first().numRows()
        Some((initPartitionRatio * rowCount / spillMinThreshold).toInt)
      case _ =>
        None
    }
  }
}