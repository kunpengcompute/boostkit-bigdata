/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.exchange.estimator

import com.huawei.boostkit.spark.util.OmniAdaptorUtil

import org.apache.spark.sql.execution.adaptive.ock.common.RuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.exchange.BoostTuningColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

case class ColumnarSamplePartitionEstimator(executionMem: Long) extends PartitionEstimator {

  override def estimatorType: EstimatorType = DataSizeBased

  override def apply(exchange: ShuffleExchangeLike): Option[Int] = {
    if (!sampleEnabled) {
      return None
    }

    exchange match {
      case ex: BoostTuningColumnarShuffleExchangeExec =>
        val inputPartitionNum = ex.inputColumnarRDD.getNumPartitions
        val sampleRDD = ex.inputColumnarRDD
          .sample(withReplacement = false, sampleRDDFraction)
          .map(cb => OmniAdaptorUtil.transColBatchToOmniVecs(cb).map(_.getCapacityInBytes).sum)
        Some(SamplePartitionEstimator(executionMem).sampleAndGenPartitionNum(ex, inputPartitionNum, sampleRDD))
      case _ =>
        None
    }
  }
}