/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import org.apache.spark.SparkContext
import org.apache.spark.util.{OCKConf, OCKFunctions, OCKRemoteFunctions}

class OckColumnarFunctions(
    mode: String,
    conf: OCKConf) {
  var shuffleMode: String = mode
  var ockConf: OCKConf = conf

  def setShuffleCompress(enable: Boolean, compressCodec: String): Unit = {
    var codec: String = "zstd"
    if (compressCodec != null) {
      codec = compressCodec
    }
    if (ShuffleMode.RSS == shuffleMode) {
      OCKRemoteFunctions.setShuffleCompress(enable, codec)
    } else {
      OCKFunctions.setShuffleCompress(enable, codec)
    }
  }

  def shuffleInitialize(): Unit = {
    if (ShuffleMode.RSS == shuffleMode) {
      OCKRemoteFunctions.shuffleInitialize(ockConf)
    } else {
      OCKFunctions.shuffleInitialize(ockConf)
    }
  }

  def genAppId(attemptId: Option[String]): String = {
    val _attemptId: String = {
      if (attemptId.nonEmpty) {
        attemptId.get
      }
      if (SparkContext.getActive.isDefined) {
        SparkContext.getActive.get.applicationAttemptId.getOrElse("1")
      }
      "1"
    }

    if (ShuffleMode.RSS == shuffleMode) {
      OCKRemoteFunctions.genAppId(ockConf.sparkConf.getAppId, _attemptId)
    } else {
      OCKFunctions.genAppId(ockConf.sparkConf.getAppId, _attemptId)
    }
  }
}