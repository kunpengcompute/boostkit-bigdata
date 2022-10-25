/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

class OckColumnarShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V],
    secureId: String,
    _appAttemptId: String)
  extends BaseShuffleHandle(shuffleId, dependency) {
  var secCode: String = secureId

  def appAttemptId : String = _appAttemptId
}