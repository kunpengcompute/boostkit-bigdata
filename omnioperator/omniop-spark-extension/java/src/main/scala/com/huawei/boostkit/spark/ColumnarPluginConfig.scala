package com.huawei.boostkit.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

class ColumnarPluginConfig(conf: SQLConf) extends Logging{
  // enable or disable columnar exchange
  val enableColumnarShuffle: Boolean = conf
    .getConfString("spark.shuffle.manager", "sort")
    .equals("org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  // enable or disable columnar hashagg
  val enableColumnarHashAgg: Boolean =
    conf.getConfString("spark.omni.sql.columnar.hashagg", "true").toBoolean

  val enableColumnarProject: Boolean =
    conf.getConfString("spark.omni.sql.columnar.project", "true").toBoolean

  val enableColumnarProjFilter: Boolean =
    conf.getConfString("spark.omni.sql.columnar.projfilter", "true").toBoolean

  val enableColumnarFilter: Boolean =
    conf.getConfString("spark.omni.sql.columnar.filter", "true").toBoolean

  // enable or disable columnar sort
  val enableColumnarSort: Boolean =
    conf.getConfString("spark.omni.sql.columnar.sort", "true").toBoolean

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

  // enable native table scan
  val enableColumnarFileScan: Boolean = conf
    .getConfString("spark.omni.sql.columnar.nativefilescan", "true")
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
  val joinOptimizationThrottle: Int =
    conf.getConfString("spark.omni.sql.columnar.joinOptimizationLevel", "12").toInt

  // columnar shuffle spill batch row number
  val columnarShuffleSpillBatchRowNum: Int =
    conf.getConfString("spark.shuffle.columnar.ShuffleSpillBatchRowNum", "10000").toInt

  // columnar shuffle spill memory threshold
  val columnarShuffleSpillMemoryThreshold: Long =
    conf.getConfString("spark.shuffle.columnar.ShuffleSpillMemoryThreshold",
      "2147483648").toLong

  // columnar shuffle compress block size
  val columnarShuffleCompressBlockSize: Int =
    conf.getConfString("spark.shuffle.columnar.compressBlockSize", "65536").toInt

  // enable shuffle compress
  val enableShuffleCompress: Boolean =
    conf.getConfString("spark.shuffle.compress", "true").toBoolean

  // shuffle compress type, default lz4
  val columnarShuffleCompressionCodec: String =
    conf.getConfString("spark.io.compression.codec", "lz4").toString

  // columnar shuffle native buffer size
  val columnarShuffleNativeBufferSize: Int =
    conf.getConfString("spark.sql.execution.columnar.maxRecordsPerBatch", "4096").toInt

  // columnar sort spill threshold
  val columnarSortSpillRowThreshold: Int =
    conf.getConfString("spark.omni.sql.columnar.sortSpill.rowThreshold", "200000").toInt

  // columnar sort spill dir disk reserve Size, default 10GB
  val columnarSortSpillDirDiskReserveSize: Long =
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
    .getConfString("spark.omni.sql.columnar.fusion", "true")
    .toBoolean

  // Pick columnar shuffle hash join if one side join count > = 0 to build local hash map, and is
  // bigger than the other side join count, and `spark.sql.join.columnar.preferShuffledHashJoin`
  // is true
  val columnarPreferShuffledHashJoin: Boolean =
    conf.getConfString("spark.sql.join.columnar.preferShuffledHashJoin", "false").toBoolean

  val maxBatchSizeInBytes: Int =
    conf.getConfString("spark.sql.columnar.maxBatchSizeInBytes", "2097152").toInt

  val maxRowCount: Int =
    conf.getConfString("spark.sql.columnar.maxRowCount", "20000").toInt

  val enableJit: Boolean = conf.getConfString("spark.omni.sql.columnar.jit", "false").toBoolean

  val enableDecimalCheck: Boolean = conf.getConfString("spark.omni.sql.decimal.constraint.check", "true").toBoolean
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
