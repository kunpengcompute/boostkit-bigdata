/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.sort.ColumnarShuffleManager
import org.apache.spark.sql.internal.SQLConf

class ColumnarPluginConfig(conf: SQLConf) extends Logging {
  // enable or disable columnar exchange
  val columnarShuffleStr:String = conf
    .getConfString("spark.shuffle.manager", "sort")

  val enableColumnarShuffle: Boolean = 
    if (!(columnarShuffleStr.equals("sort") || (columnarShuffleStr.equals("tungsten-sort")))) {
      SparkEnv.get.shuffleManager.isInstanceOf[ColumnarShuffleManager]
    } else {
      false
    }

  // enable or disable columnar hashagg
  val enableColumnarHashAgg: Boolean =
    conf.getConfString("spark.omni.sql.columnar.hashagg", "true").toBoolean

  val enableColumnarProject: Boolean =
    conf.getConfString("spark.omni.sql.columnar.project", "true").toBoolean

  val enableColumnarProjFilter: Boolean =
    conf.getConfString("spark.omni.sql.columnar.projfilter", "true").toBoolean

  val enableColumnarFilter: Boolean =
    conf.getConfString("spark.omni.sql.columnar.filter", "true").toBoolean

  val enableColumnarExpand: Boolean =
    conf.getConfString("spark.omni.sql.columnar.expand", "true").toBoolean

  // enable or disable columnar sort
  val enableColumnarSort: Boolean =
    conf.getConfString("spark.omni.sql.columnar.sort", "true").toBoolean

  // enable or disable topNSort
  val enableColumnarTopNSort: Boolean =
    conf.getConfString("spark.omni.sql.columnar.topnsort", "true").toBoolean

  val topNSortThreshold: Int =
    conf.getConfString("spark.omni.sql.columnar.topnsortthreshold", "100").toInt

  val enableColumnarUnion: Boolean =
    conf.getConfString("spark.omni.sql.columnar.union", "true").toBoolean

  // enable or disable columnar window
  val enableColumnarWindow: Boolean =
    conf.getConfString("spark.omni.sql.columnar.window", "true").toBoolean

  // enable or disable columnar broadcastexchange
  val enableColumnarBroadcastExchange: Boolean =
    conf.getConfString("spark.omni.sql.columnar.broadcastexchange", "true").toBoolean

  // enable or disable columnar wholestagecodegen
  val enableColumnarWholeStageCodegen: Boolean =
    conf.getConfString("spark.omni.sql.columnar.wholestagecodegen", "true").toBoolean

  // enable or disable columnar BroadcastHashJoin
  val enableColumnarBroadcastJoin: Boolean = conf
    .getConfString("spark.omni.sql.columnar.broadcastJoin", "true")
    .toBoolean

  // enable or disable heuristic join reorder
  val enableHeuristicJoinReorder: Boolean =
    conf.getConfString("spark.sql.heuristicJoinReorder.enabled", "true").toBoolean

  // enable or disable delay cartesian product
  val enableDelayCartesianProduct: Boolean =
    conf.getConfString("spark.sql.enableDelayCartesianProduct.enabled", "true").toBoolean

  // enable native table scan
  val enableColumnarFileScan: Boolean = conf
    .getConfString("spark.omni.sql.columnar.nativefilescan", "true")
    .toBoolean

  // enable native table scan
  val enableOrcNativeFileScan: Boolean = conf
    .getConfString("spark.omni.sql.columnar.orcNativefilescan", "true")
    .toBoolean

  val enableColumnarSortMergeJoin: Boolean = conf
    .getConfString("spark.omni.sql.columnar.sortMergeJoin", "true")
    .toBoolean

  val enableTakeOrderedAndProject: Boolean = conf
    .getConfString("spark.omni.sql.columnar.takeOrderedAndProject", "true").toBoolean

  val enableShuffleBatchMerge: Boolean = conf
    .getConfString("spark.omni.sql.columnar.shuffle.merge", "true").toBoolean

  val enableJoinBatchMerge: Boolean = conf
    .getConfString("spark.omni.sql.columnar.broadcastJoin.merge", "false").toBoolean

  val enableSortMergeJoinBatchMerge: Boolean = conf
    .getConfString("spark.omni.sql.columnar.sortMergeJoin.merge", "true").toBoolean

  // prefer to use columnar operators if set to true
  val enablePreferColumnar: Boolean =
    conf.getConfString("spark.omni.sql.columnar.preferColumnar", "true").toBoolean

  // fallback to row operators if there are several continous joins
  val joinOptimizationThrottle: Integer =
    conf.getConfString("spark.omni.sql.columnar.joinOptimizationLevel", "12").toInt

  // columnar shuffle spill batch row number
  val columnarShuffleSpillBatchRowNum =
    conf.getConfString("spark.shuffle.columnar.shuffleSpillBatchRowNum", "10000").toInt

  // columnar shuffle spill memory threshold
  val columnarShuffleSpillMemoryThreshold =
    conf.getConfString("spark.shuffle.columnar.shuffleSpillMemoryThreshold",
      "2147483648").toLong

  // columnar shuffle compress block size
  val columnarShuffleCompressBlockSize =
    conf.getConfString("spark.shuffle.columnar.compressBlockSize", "65536").toInt

  // enable shuffle compress
  val enableShuffleCompress =
    conf.getConfString("spark.shuffle.compress", "true").toBoolean

  // shuffle compress type, default lz4
  val columnarShuffleCompressionCodec =
    conf.getConfString("spark.io.compression.codec", "lz4").toString

  // columnar shuffle native buffer size
  val columnarShuffleNativeBufferSize =
    conf.getConfString("spark.sql.execution.columnar.maxRecordsPerBatch", "4096").toInt

  // columnar sort spill threshold
  val columnarSortSpillRowThreshold: Integer =
    conf.getConfString("spark.omni.sql.columnar.sortSpill.rowThreshold", "200000").toInt

  // columnar sort spill dir disk reserve Size, default 10GB
  val columnarSortSpillDirDiskReserveSize:Long =
    conf.getConfString("spark.omni.sql.columnar.sortSpill.dirDiskReserveSize", "10737418240").toLong

  // enable or disable columnar sortSpill
  val enableSortSpill: Boolean = conf
    .getConfString("spark.omni.sql.columnar.sortSpill.enabled", "false")
    .toBoolean

  // enable or disable columnar shuffledHashJoin
  val enableShuffledHashJoin: Boolean = conf
    .getConfString("spark.omni.sql.columnar.shuffledHashJoin", "true")
    .toBoolean

  val enableFusion: Boolean = conf
    .getConfString("spark.omni.sql.columnar.fusion", "false")
    .toBoolean

  // Pick columnar shuffle hash join if one side join count > = 0 to build local hash map, and is
  // bigger than the other side join count, and `spark.sql.join.columnar.preferShuffledHashJoin`
  // is true.
  val columnarPreferShuffledHashJoin =
    conf.getConfString("spark.sql.join.columnar.preferShuffledHashJoin", "false").toBoolean

  val maxBatchSizeInBytes =
    conf.getConfString("spark.sql.columnar.maxBatchSizeInBytes", "2097152").toInt

  val maxRowCount =
    conf.getConfString("spark.sql.columnar.maxRowCount", "20000").toInt

  val enableColumnarUdf: Boolean = conf.getConfString("spark.omni.sql.columnar.udf", "true").toBoolean

  val enableOmniExpCheck : Boolean = conf.getConfString("spark.omni.sql.omniExp.check", "true").toBoolean

  val enableColumnarProjectFusion : Boolean = conf.getConfString("spark.omni.sql.columnar.projectFusion", "true").toBoolean
}


object ColumnarPluginConfig {
  var ins: ColumnarPluginConfig = null

  def getConf: ColumnarPluginConfig = synchronized {
    if (ins == null) {
      ins = getSessionConf
    }
    ins
  }

  def getSessionConf: ColumnarPluginConfig = {
    new ColumnarPluginConfig(SQLConf.get)
  }
}
