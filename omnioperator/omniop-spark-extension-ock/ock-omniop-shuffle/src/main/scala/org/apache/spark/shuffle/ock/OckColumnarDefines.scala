/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.apache.spark.shuffle.ock

object OckColumnarDefines {
  val ShuffleModeKey = "spark.shuffle.ock.mode"
}

object ShuffleMode extends Enumeration {
  val ESS = "ess"
  val RSS = "rss"
}