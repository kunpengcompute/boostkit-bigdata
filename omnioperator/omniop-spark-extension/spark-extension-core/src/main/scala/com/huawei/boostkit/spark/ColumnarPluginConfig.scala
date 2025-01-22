/*
 * Copyright (C) 2020-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.ColumnarPluginConfig._
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.sort.ColumnarShuffleManager
import org.apache.spark.sql.internal.SQLConf

class ColumnarPluginConfig(conf: SQLConf) extends Logging {
  def columnarShuffleStr: String = conf
    .getConfString("spark.shuffle.manager", "sort")

  def enableColumnarShuffle: Boolean =
    if (!(columnarShuffleStr.equals("sort") || (columnarShuffleStr.equals("tungsten-sort")))) {
      SparkEnv.get.shuffleManager.isInstanceOf[ColumnarShuffleManager]
    } else {
      false
    }

  def enableColumnarHashAgg: Boolean = conf.getConf(ENABLE_COLUMNAR_HASH_AGG)

  def enableColumnarProject: Boolean = conf.getConf(ENABLE_COLUMNAR_PROJECT)

  def enableColumnarProjFilter: Boolean = conf.getConf(ENABLE_COLUMNAR_PROJ_FILTER)

  def enableColumnarFilter: Boolean = conf.getConf(ENABLE_COLUMNAR_FILTER)

  def enableColumnarExpand: Boolean = conf.getConf(ENABLE_COLUMNAR_EXPAND)

  def enableColumnarSort: Boolean =  conf.getConf(ENABLE_COLUMNAR_SORT)

  def enableColumnarTopNSort: Boolean = conf.getConf(ENABLE_COLUMNAR_TOP_N_SORT)

  def enableColumnarUnion: Boolean = conf.getConf(ENABLE_COLUMNAR_UNION)

  def enableColumnarWindow: Boolean = conf.getConf(ENABLE_COLUMNAR_WINDOW)

  def enableColumnarBroadcastExchange: Boolean = conf.getConf(ENABLE_COLUMNAR_BROADCAST_EXCHANGE)

  def enableColumnarWholeStageCodegen: Boolean = conf.getConf(ENABLE_COLUMNAR_WHOLE_STAGE_CODEGEN)

  def enableColumnarBroadcastJoin: Boolean = conf.getConf(ENABLE_COLUMNAR_BROADCAST_JOIN)

  def enableColumnarBroadcastNestedJoin: Boolean = conf.getConf(ENABLE_COLUMNAR_BROADCAST_NESTED_JOIN)

  def enableShareBroadcastJoinHashTable: Boolean = conf.getConf(ENABLE_SHARE_BROADCAST_JOIN_HASH_TABLE)

  def enableShareBroadcastJoinNestedTable: Boolean = conf.getConf(ENABLE_SHARE_BROADCAST_JOIN_NESTED_TABLE)

  def enableDelayCartesianProduct: Boolean = conf.getConf(ENABLE_DELAY_CARTESIAN_PRODUCT)

  def enableColumnarFileScan: Boolean = conf.getConf(ENABLE_COLUMNAR_FILE_SCAN)

  def enableOrcNativeFileScan: Boolean = conf.getConf(ENABLE_ORC_NATIVE_FILE_SCAN)

  def enableParquetNativeFileScan: Boolean = conf.getConf(ENABLE_PARQUET_NATIVE_FILE_SCAN)

  def enableColumnarSortMergeJoin: Boolean = conf.getConf(ENABLE_COLUMNAR_SORT_MERGE_JOIN)

  def enableColumnarDataWritingCommand: Boolean = conf.getConf(ENABLE_COLUMNAR_DATA_WRITING_COMMAND)

  def enableTakeOrderedAndProject: Boolean = conf.getConf(ENABLE_TAKE_ORDERED_AND_PROJECT)

  def enableShuffleBatchMerge: Boolean = conf.getConf(ENABLE_SHUFFLE_BATCH_MERGE)

  def enableJoinBatchMerge: Boolean =  conf.getConf(ENABLE_JOIN_BATCH_MERGE)

  def enableSortMergeJoinBatchMerge: Boolean = conf.getConf(ENABLE_SORT_MERGE_JOIN_BATCH_MERGE)

  def enablePreferColumnar: Boolean = conf.getConf(ENABLE_PREFER_COLUMNAR)

  def joinOptimizationThrottle: Int = conf.getConf(JOIN_OPTIMIZATION_THROTTLE)

  def columnarShuffleSpillBatchRowNum: Int = conf.getConf(COLUMNAR_SHUFFLE_SPILL_BATCH_ROW_NUM)

  def columnarShuffleTaskSpillMemoryThreshold: Long = conf.getConf(COLUMNAR_SHUFFLE_TASK_SPILL_MEMORY_THRESHOLD)

  def columnarShuffleCompressBlockSize: Int = conf.getConf(COLUMNAR_SHUFFLE_COMPRESS_BLOCK_SIZE)

  def enableShuffleCompress: Boolean = conf.getConfString("spark.shuffle.compress", "true").toBoolean

  def columnarShuffleCompressionCodec: String = conf.getConfString("spark.io.compression.codec", "lz4")

  def columnarShuffleNativeBufferSize: Int = conf.getConf(COLUMNAR_SHUFFLE_NATIVE_BUFFER_SIZE)

  def columnarSpillWriteBufferSize: Long = conf.getConf(COLUMNAR_SPILL_WRITE_BUFFER_SIZE)

  def columnarSpillMemPctThreshold: Int = conf.getConf(COLUMNAR_SPILL_MEM_PCT_THRESHOLD)

  def columnarSpillDirDiskReserveSize: Long = conf.getConf(COLUMNAR_SPILL_DIR_DISK_RESERVE_SIZE)

  def enableSortSpill: Boolean = conf.getConf(ENABLE_SORT_SPILL)

  def columnarSortSpillRowThreshold: Int = conf.getConf(COLUMNAR_SORT_SPILL_ROW_THRESHOLD)

  def enableWindowSpill: Boolean = conf.getConf(ENABLE_WINDOW_SPILL)

  def columnarWindowSpillRowThreshold: Int = conf.getConf(COLUMNAR_WINDOW_SPILL_ROW_THRESHOLD)

  def enableHashAggSpill: Boolean = conf.getConf(ENABLE_HASH_AGG_SPILL)

  def columnarHashAggSpillRowThreshold: Int = conf.getConf(COLUMNAR_HASH_AGG_SPILL_ROW_THRESHOLD)

  def enableShuffledHashJoin: Boolean = conf.getConf(ENABLE_SHUFFLED_HASH_JOIN)

  def forceShuffledHashJoin: Boolean = conf.getConf(FORCE_SHUFFLED_HASH_JOIN)

  def enableRewriteSelfJoinInInPredicate: Boolean = conf.getConf(ENABLE_REWRITE_SELF_JOIN_IN_IN_PREDICATE)

  def enableFusion: Boolean = conf.getConf(ENABLE_FUSION)

  def columnarPreferShuffledHashJoin: Boolean = conf.getConf(COLUMNAR_PREFER_SHUFFLED_HASH_JOIN)

  def maxBatchSizeInBytes: Int = conf.getConf(MAX_BATCH_SIZE_IN_BYTES)

  def maxRowCount: Int = conf.getConf(MAX_ROW_COUNT)

  def mergedBatchThreshold: Int = conf.getConf(MERGED_BATCH_THRESHOLD)

  def enableColumnarUdf: Boolean = conf.getConf(ENABLE_COLUMNAR_UDF)

  def enableOmniExpCheck : Boolean = conf.getConf(ENABLE_OMNI_EXP_CHECK)

  def enableColumnarProjectFusion : Boolean = conf.getConf(ENABLE_COLUMNAR_PROJECT_FUSION)

  def enableLocalColumnarLimit : Boolean = conf.getConf(ENABLE_LOCAL_COLUMNAR_LIMIT)

  def enableGlobalColumnarLimit : Boolean = conf.getConf(ENABLE_GLOBAL_COLUMNAR_LIMIT)

  def topNSortThreshold: Int = conf.getConf(TOP_N_THRESHOLD)

  def pushOrderedLimitThroughAggEnable: Boolean =  conf.getConf(PUSH_ORDERED_LIMIT_THROUGH_AGG_ENABLE)

  def enableDedupLeftSemiJoin: Boolean = conf.getConf(ENABLE_DEDUP_LEFT_SEMI_JOIN)

  def dedupLeftSemiJoinThreshold: Int = conf.getConf(DEDUP_LEFT_SEMI_JOIN_THRESHOLD)

  def combineJoinedAggregatesEnabled: Boolean = conf.getConf(COMBINE_JOINED_AGGREGATES_ENABLED)

  def filterMergeEnable: Boolean = conf.getConf(FILTER_MERGE_ENABLE)

  def filterMergeThreshold: Double = conf.getConf(FILTER_MERGE_THRESHOLD)

  def enableColumnarCoalesce: Boolean = conf.getConf(ENABLE_COLUMNAR_COALESCE)

  def enableRadixSort: Boolean = conf.getConf(ENABLE_RADIX_SORT)

  def enableRollupOptimization: Boolean = conf.getConf(ENABLE_ROLLUP_OPTIMIZATION)

  def radixSortThreshold: Int = conf.getConf(RADIX_SORT_THRESHOLD)

  def enableRowShuffle: Boolean = conf.getConf(ENABLE_ROW_SHUFFLE)

  def columnsThreshold: Int = conf.getConf(COLUMNS_THRESHOLD)

  def enableBloomfilterSubqueryReuse: Boolean = conf.getConf(ENABLE_BLOOMFILTER_SUBQUERY_REUSE)

  def wholeStageFallbackThreshold: Int = conf.getConf(WHOLE_STAGE_FALLBACK_THRESHOLD)

  def queryFallbackThreshold: Int = conf.getConf(QUERY_FALLBACK_THRESHOLD)

  def enableAdaptivePartialAggregation: Boolean = conf.getConf(ENABLE_ADAPTIVE_PARTIAL_AGGREGATION)

  def adaptivePartialAggregationMinRows: Int = conf.getConf(ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS)

  def adaptivePartialAggregationRatio: Double = conf.getConf(ADAPTIVE_PARTIAL_AGGREGATION_RATIO)

  def timeParserPolicy: String = conf.getConfString("spark.sql.legacy.timeParserPolicy")

  def enableOmniUnixTimeFunc: Boolean = conf.getConf(ENABLE_OMNI_UNIXTIME_FUNCTION)
}


object ColumnarPluginConfig {

  import SQLConf._

  val OMNI_ENABLE_KEY: String = "spark.omni.enabled"

  buildConf(OMNI_ENABLE_KEY)
    .internal()
    .booleanConf
    .createWithDefault(true)

  val OMNI_BROADCAST_EXCHANGE_ENABLE_KEY = "spark.omni.sql.columnar.broadcastexchange"

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

  val ENABLE_COLUMNAR_HASH_AGG = buildConf("spark.omni.sql.columnar.hashagg")
    .internal()
    .doc("enable or disable columnar hashagg")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_PROJECT = buildConf("spark.omni.sql.columnar.project")
    .internal()
    .doc("enable or disable columnar project")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_PROJ_FILTER =  buildConf("spark.omni.sql.columnar.projfilter")
    .internal()
    .doc("enable or disable columnar project filter")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_FILTER = buildConf("spark.omni.sql.columnar.filter")
    .internal()
    .doc("enable or disable columnar filter")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_EXPAND = buildConf("spark.omni.sql.columnar.expand")
    .internal()
    .doc("enable or disable columnar project expand")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_SORT = buildConf("spark.omni.sql.columnar.sort")
    .internal()
    .doc("enable or disable columnar project sort")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_TOP_N_SORT = buildConf("spark.omni.sql.columnar.topNSort")
    .internal()
    .doc("enable or disable columnar TopNSort")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_UNION = buildConf("spark.omni.sql.columnar.union")
    .internal()
    .doc("enable or disable columnar union")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_WINDOW = buildConf("spark.omni.sql.columnar.window")
    .internal()
    .doc("enable or disable columnar window")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_BROADCAST_EXCHANGE = buildConf(OMNI_BROADCAST_EXCHANGE_ENABLE_KEY)
    .internal()
    .doc("enable or disable columnar broadcastexchange")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_WHOLE_STAGE_CODEGEN = buildConf("spark.omni.sql.columnar.wholestagecodegen")
    .internal()
    .doc("enable or disable columnar wholestagecodegen")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_BROADCAST_JOIN = buildConf("spark.omni.sql.columnar.broadcastJoin")
    .internal()
    .doc("enable or disable columnar broadcastJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_BROADCAST_NESTED_JOIN = buildConf("spark.omni.sql.columnar.broadcastNestedJoin")
    .internal()
    .doc("enable or disable columnar broadcastNestedJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_SHARE_BROADCAST_JOIN_HASH_TABLE = buildConf("spark.omni.sql.columnar.broadcastJoin.sharehashtable")
    .internal()
    .doc("enable or disable share columnar BroadcastHashJoin hashtable")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_SHARE_BROADCAST_JOIN_NESTED_TABLE = buildConf("spark.omni.sql.columnar.broadcastNestedJoin.shareBuildTable")
    .internal()
    .doc("enable or disable share columnar broadcastNestedJoin buildtable")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_DELAY_CARTESIAN_PRODUCT = buildConf("spark.sql.enableDelayCartesianProduct.enabled")
    .internal()
    .doc("enable or disable delay cartesian product")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_FILE_SCAN = buildConf("spark.omni.sql.columnar.nativefilescan")
    .internal()
    .doc("enable native table scan")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_NATIVE_FILE_SCAN = buildConf("spark.omni.sql.columnar.orcNativefilescan")
    .internal()
    .doc("enable orc native table scan")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_PARQUET_NATIVE_FILE_SCAN = buildConf("spark.omni.sql.columnar.parquetNativefilescan")
    .internal()
    .doc("enable parquet native table scan")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_SORT_MERGE_JOIN = buildConf("spark.omni.sql.columnar.sortMergeJoin")
    .internal()
    .doc("enable columnar sortMergeJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_DATA_WRITING_COMMAND = buildConf("spark.omni.sql.columnar.dataWritingCommand")
    .internal()
    .doc("enable columnar dataWritingCommand")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_TAKE_ORDERED_AND_PROJECT = buildConf("spark.omni.sql.columnar.takeOrderedAndProject")
    .internal()
    .doc("enable columnar takeOrderedAndProject")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_SHUFFLE_BATCH_MERGE = buildConf("spark.omni.sql.columnar.shuffle.merge")
    .internal()
    .doc("enable columnar shuffle merge")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_JOIN_BATCH_MERGE = buildConf("spark.omni.sql.columnar.broadcastJoin.merge")
    .internal()
    .doc("enable columnar broadcastJoin merge")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_SORT_MERGE_JOIN_BATCH_MERGE = buildConf("spark.omni.sql.columnar.sortMergeJoin.merge")
    .internal()
    .doc("enable columnar sortMergeJoin merge")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_PREFER_COLUMNAR = buildConf("spark.omni.sql.columnar.preferColumnar")
    .internal()
    .doc("prefer to use columnar operators if set to true")
    .booleanConf
    .createWithDefault(true)

  val JOIN_OPTIMIZATION_THROTTLE = buildConf("spark.omni.sql.columnar.joinOptimizationLevel")
    .internal()
    .doc("fallback to row operators if there are several continous joins")
    .intConf
    .createWithDefault(12)

  val COLUMNAR_SHUFFLE_SPILL_BATCH_ROW_NUM = buildConf("spark.shuffle.columnar.shuffleSpillBatchRowNum")
    .internal()
    .doc("columnar shuffle spill batch row number")
    .intConf
    .createWithDefault(10000)

  val COLUMNAR_SHUFFLE_TASK_SPILL_MEMORY_THRESHOLD = buildConf("spark.shuffle.columnar.shuffleTaskSpillMemoryThreshold")
    .internal()
    .doc("columnar shuffle spill memory threshold in task level")
    .longConf
    .createWithDefault(2147483648L)

  val COLUMNAR_SHUFFLE_COMPRESS_BLOCK_SIZE = buildConf("spark.shuffle.columnar.compressBlockSize")
    .internal()
    .doc("columnar shuffle compress block size")
    .intConf
    .createWithDefault(65536)

  val COLUMNAR_SHUFFLE_NATIVE_BUFFER_SIZE = buildConf("spark.sql.execution.columnar.maxRecordsPerBatch")
    .internal()
    .doc("columnar shuffle native buffer size")
    .intConf
    .createWithDefault(4096)

  val COLUMNAR_SPILL_WRITE_BUFFER_SIZE = buildConf("spark.omni.sql.columnar.spill.writeBufferSize")
    .internal()
    .doc("columnar spill native buffer size")
    .longConf
    .createWithDefault(4121440L)

  val COLUMNAR_SPILL_MEM_PCT_THRESHOLD = buildConf("spark.omni.sql.columnar.spill.memFraction")
    .internal()
    .doc("columnar spill threshold - Percentage of memory usage," +
      " associate with the \"spark.memory.offHeap\" together")
    .intConf
    .createWithDefault(90)

  val COLUMNAR_SPILL_DIR_DISK_RESERVE_SIZE = buildConf("spark.omni.sql.columnar.spill.dirDiskReserveSize")
    .internal()
    .doc("columnar spill dir disk reserve Size, default 10GB")
    .longConf
    .createWithDefault(10737418240L)

  val ENABLE_SORT_SPILL = buildConf("spark.omni.sql.columnar.sortSpill.enabled")
    .internal()
    .doc("enable or disable columnar sort spill")
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_SORT_SPILL_ROW_THRESHOLD = buildConf("spark.omni.sql.columnar.sortSpill.rowThreshold")
    .internal()
    .doc("columnar sort spill threshold")
    .intConf
    .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_WINDOW_SPILL = buildConf("spark.omni.sql.columnar.windowSpill.enabled")
    .internal()
    .doc("enable or disable columnar window spill")
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_WINDOW_SPILL_ROW_THRESHOLD = buildConf("spark.omni.sql.columnar.windowSpill.rowThreshold")
    .internal()
    .doc("columnar window spill threshold")
    .intConf
    .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_HASH_AGG_SPILL = buildConf("spark.omni.sql.columnar.hashAggSpill.enabled")
    .internal()
    .doc("enable or disable columnar hash aggregate spill")
    .booleanConf
    .createWithDefault(true)

  val COLUMNAR_HASH_AGG_SPILL_ROW_THRESHOLD = buildConf("spark.omni.sql.columnar.hashAggSpill.rowThreshold")
    .internal()
    .doc("columnar hash aggregate spill threshold")
    .intConf
    .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_SHUFFLED_HASH_JOIN = buildConf("spark.omni.sql.columnar.shuffledHashJoin")
    .internal()
    .doc("enable or disable columnar shuffledHashJoin")
    .booleanConf
    .createWithDefault(true)

  val FORCE_SHUFFLED_HASH_JOIN = buildConf("spark.omni.sql.columnar.forceShuffledHashJoin")
    .internal()
    .doc("enable or disable force shuffle hash join")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_REWRITE_SELF_JOIN_IN_IN_PREDICATE = buildConf("spark.omni.sql.columnar.RewriteSelfJoinInInPredicate")
    .internal()
    .doc("enable or disable rewrite self join in Predicate to aggregate")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_FUSION = buildConf("spark.omni.sql.columnar.fusion")
    .internal()
    .doc("enable or disable columnar fusion")
    .booleanConf
    .createWithDefault(false)

  val COLUMNAR_PREFER_SHUFFLED_HASH_JOIN = buildConf("spark.sql.join.columnar.preferShuffledHashJoin")
    .internal()
    .doc("Pick columnar shuffle hash join if one side join count > = 0 to build local hash map, " +
      "and is bigger than the other side join count, and `spark.sql.join.columnar.preferShuffledHashJoin`" +
      "is true.")
    .booleanConf
    .createWithDefault(false)

  val MAX_BATCH_SIZE_IN_BYTES = buildConf("spark.sql.columnar.maxBatchSizeInBytes")
    .internal()
    .intConf
    .createWithDefault(2097152)

  val MAX_ROW_COUNT = buildConf("spark.sql.columnar.maxRowCount")
    .internal()
    .intConf
    .createWithDefault(20000)

  val MERGED_BATCH_THRESHOLD = buildConf("spark.sql.columnar.mergedBatchThreshold")
    .internal()
    .intConf
    .createWithDefault(100)

  val ENABLE_COLUMNAR_UDF = buildConf("spark.omni.sql.columnar.udf")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_OMNI_EXP_CHECK = buildConf("spark.omni.sql.omniExp.check")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COLUMNAR_PROJECT_FUSION = buildConf("spark.omni.sql.columnar.projectFusion")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_LOCAL_COLUMNAR_LIMIT = buildConf("spark.omni.sql.columnar.localLimit")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_GLOBAL_COLUMNAR_LIMIT = buildConf("spark.omni.sql.columnar.globalLimit")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val TOP_N_THRESHOLD = buildConf("spark.omni.sql.columnar.topN.threshold")
    .internal()
    .intConf
    .createWithDefault(100)

  val PUSH_ORDERED_LIMIT_THROUGH_AGG_ENABLE = buildConf("spark.omni.sql.columnar.pushOrderedLimitThroughAggEnable.enabled")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ENABLE_DEDUP_LEFT_SEMI_JOIN = buildConf("spark.omni.sql.columnar.dedupLeftSemiJoin")
    .internal()
    .doc("enable or disable deduplicate the right side of left semi join")
    .booleanConf
    .createWithDefault(false)

  val DEDUP_LEFT_SEMI_JOIN_THRESHOLD = buildConf("spark.omni.sql.columnar.dedupLeftSemiJoinThreshold")
    .internal()
    .intConf
    .createWithDefault(3)

  val COMBINE_JOINED_AGGREGATES_ENABLED = buildConf("spark.omni.sql.columnar.combineJoinedAggregates.enabled")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val FILTER_MERGE_ENABLE = buildConf("spark.sql.execution.filterMerge.enabled")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val FILTER_MERGE_THRESHOLD = buildConf("spark.sql.execution.filterMerge.maxCost")
    .internal()
    .doubleConf
    .createWithDefault(100.0)

  val ENABLE_COLUMNAR_COALESCE = buildConf("spark.omni.sql.columnar.coalesce")
    .internal()
    .doc("enable or disable columnar CoalesceExec")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ROLLUP_OPTIMIZATION = buildConf("spark.omni.sql.columnar.rollupOptimization.enabled")
    .internal()
    .doc("enable or disable columnar rollupOptimization")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RADIX_SORT = buildConf("spark.omni.sql.columnar.radixSort.enabled")
    .internal()
    .doc("enable or disable radix sort")
    .booleanConf
    .createWithDefault(false)

  val RADIX_SORT_THRESHOLD = buildConf("spark.omni.sql.columnar.radixSortThreshold")
    .internal()
    .intConf
    .createWithDefault(1000000)

  val ENABLE_ROW_SHUFFLE = buildConf("spark.omni.sql.columnar.rowShuffle.enabled")
    .internal()
    .doc("enable or disable row shuffle")
    .booleanConf
    .createWithDefault(true)

  val COLUMNS_THRESHOLD = buildConf("spark.omni.sql.columnar.columnsThreshold")
    .internal()
    .intConf
    .createWithDefault(10)

  val ENABLE_BLOOMFILTER_SUBQUERY_REUSE = buildConf("spark.omni.sql.columnar.bloomfilterSubqueryReuse")
    .internal()
    .doc("enable or disable bloomfilter subquery reuse")
    .booleanConf
    .createWithDefault(false)

  val WHOLE_STAGE_FALLBACK_THRESHOLD = buildConf("spark.omni.sql.columnar.wholeStage.fallback.threshold")
    .internal()
    .doc("The threshold for whether whole stage will fall back in AQE supported case by counting the number of vanilla SparkPlan." +
      "If it is set to -1, it means that this function is turned off" +
      "otherwise, when the number of vanilla SparkPlan of the stage is greater than or equal to the threshold," +
      "all the SparkPlan of the stage will be fallback to vanilla SparkPlan")
    .intConf
    .createWithDefault(-1)

  val QUERY_FALLBACK_THRESHOLD = buildConf("spark.omni.sql.columnar.query.fallback.threshold")
    .internal()
    .doc("it is same with wholeStageFallbackThreshold, but it is used for non AQE")
    .intConf
    .createWithDefault(-1)

  val ENABLE_OMNI_COLUMNAR_TO_ROW = buildConf("spark.omni.sql.columnar.columnarToRow")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ADAPTIVE_PARTIAL_AGGREGATION = buildConf("spark.omni.sql.columnar.adaptivePartialAggregation.enabled")
    .internal()
    .doc("enable or disable adaptive partial aggregation")
    .booleanConf
    .createWithDefault(false)

  val ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS = buildConf("spark.omni.sql.columnar.adaptivePartialAggregationMinRows")
    .internal()
    .intConf
    .createWithDefault(500000)

  val ADAPTIVE_PARTIAL_AGGREGATION_RATIO =  buildConf("spark.omni.sql.columnar.adaptivePartialAggregationRatio")
    .internal()
    .doubleConf
    .createWithDefault(0.8)

  val ENABLE_OMNI_UNIXTIME_FUNCTION = buildConf("spark.omni.sql.columnar.unixTimeFunc.enabled")
    .internal()
    .doc("enable omni unix_timestamp and from_unixtime")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_OMNI_OP_FACTORY_CACHE = buildConf("spark.omni.sql.columnar.operator.factory.cache.enabled")
    .internal()
    .doc("enable omni opreator factory cache")
    .booleanConf
    .createWithDefault(true)

}
