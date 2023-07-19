/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.common

import org.apache.spark.SparkEnv

object OmniOpDefine {
    final val COLUMNAR_SHUFFLE_MANAGER_DEFINE = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"

    final val COLUMNAR_SORT_SPILL_ROW_THRESHOLD = "spark.omni.sql.columnar.sortSpill.rowThreshold"
    final val COLUMNAR_SORT_SPILL_ROW_BASED_ENABLED = "spark.omni.sql.columnar.sortSpill.enabled"
}

object OmniOCKShuffleDefine {
    final val OCK_COLUMNAR_SHUFFLE_MANAGER_DEFINE = "org.apache.spark.shuffle.ock.OckColumnarShuffleManager"
}

object OmniRuntimeConfiguration {
    val OMNI_SPILL_ROWS: Long = SparkEnv.get.conf.getLong(OmniOpDefine.COLUMNAR_SORT_SPILL_ROW_THRESHOLD, Integer.MAX_VALUE)
    val OMNI_SPILL_ROW_ENABLED: Boolean = SparkEnv.get.conf.getBoolean(OmniOpDefine.COLUMNAR_SORT_SPILL_ROW_BASED_ENABLED, defaultValue = true)
}