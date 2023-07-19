/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.adaptive.ock.rule._

class OmniOpBoostTuningExtension extends (SparkSessionExtensions => Unit) {
    override def apply(extensions: SparkSessionExtensions): Unit = {
        extensions.injectQueryStagePrepRule(_ => OmniOpBoostTuningQueryStagePrepRule())
        extensions.injectColumnar(_ => OmniOpBoostTuningColumnarRule(
          OmniOpBoostTuningPreColumnarRule(), OmniOpBoostTuningPostColumnarRule()))
        SparkContext.getActive.get.addSparkListener(new BoostTuningListener())
    }
}