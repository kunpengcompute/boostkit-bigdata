/*
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

package org.apache.spark.sql.execution

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP

import scala.collection.JavaConverters._
import scala.concurrent.Future
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import com.huawei.boostkit.spark.vectorized.PartitionInfo
import nova.hetu.omniruntime.`type`.{DataType, DataTypeSerializer}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.vector.{IntVec, VecBatch}
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.util.{MergeIterator, SparkMemoryUtils}
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.XORShiftRandom

case class ColumnarShuffleExchangeExec(
  override val outputPartitioning: Partitioning,
  child: SparkPlan,
  shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
  advisoryPartitionSize: Option[Long] = None,
  handleRow: Boolean = false)
  extends ShuffleExchangeLike {

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_compress"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numMergedVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatches"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  override def nodeName: String = if (!handleRow) "OmniColumnarShuffleExchange" else "OmniRowShuffleExchange"

  override def supportsColumnar: Boolean = true

  val serializer: Serializer = new ColumnarBatchSerializer(
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"),
    handleRow)

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputColumnarRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(columnarShuffleDependency)
    }
  }

  override def numMappers: Int = columnarShuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = columnarShuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[ColumnarBatch] = {
    new ShuffledColumnarRDD(columnarShuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  lazy val columnarShuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val dep = ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputColumnarRDD,
      child.output,
      outputPartitioning,
      serializer,
      handleRow,
      writeMetrics,
      longMetric("dataSize"),
      longMetric("bytesSpilled"),
      longMetric("numInputRows"),
      longMetric("splitTime"),
      longMetric("spillTime"))
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  private var cachedShuffleRDD: ShuffledColumnarRDD = null

  private val enableShuffleBatchMerge: Boolean = ColumnarPluginConfig.getSessionConf.enableShuffleBatchMerge

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  def buildCheck(): Unit = {
    val inputTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    outputPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        val genHashExpressionFunc = ColumnarShuffleExchangeExec.genHashExpr()
        val hashJSonExpressions = genHashExpressionFunc(expressions, numPartitions, ColumnarShuffleExchangeExec.defaultMm3HashSeed, child.output)
        if (!isSimpleColumn(hashJSonExpressions)) {
          checkOmniJsonWhiteList("", Array(hashJSonExpressions))
        }
      case _ =>
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledColumnarRDD(columnarShuffleDependency, readMetrics)
    }
    
    if (enableShuffleBatchMerge) {
      cachedShuffleRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        val mergeIterator = new MergeIterator(iter,
          localSchema = StructType(child.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))),
          longMetric("numMergedVecBatches"))
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          mergeIterator.close()
        })
        mergeIterator
      }
    } else {
      cachedShuffleRDD
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarShuffleExchangeExec =
    copy(child = newChild)
}

object ColumnarShuffleExchangeExec extends Logging {
  val defaultMm3HashSeed: Int = 42;
  val rollupConst : String = "spark_grouping_id"

  def prepareShuffleDependency(
                                rdd: RDD[ColumnarBatch],
                                outputAttributes: Seq[Attribute],
                                newPartitioning: Partitioning,
                                serializer: Serializer,
                                handleRow: Boolean,
                                writeMetrics: Map[String, SQLMetric],
                                dataSize: SQLMetric,
                                bytesSpilled: SQLMetric,
                                numInputRows: SQLMetric,
                                splitTime: SQLMetric,
                                spillTime: SQLMetric):
  ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {


    val part: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          iter.flatMap(batch => {
            val rows: Iterator[InternalRow] = batch.rowIterator.asScala
            val mutablePair = new MutablePair[InternalRow, Null]()
            new Iterator[MutablePair[InternalRow, Null]] {
              var closed = false
              override def hasNext: Boolean = {
                val has: Boolean = rows.hasNext
                if (!has && !closed) {
                  batch.close()
                  closed = true
                }
                has
              }
              override def next(): MutablePair[InternalRow, Null] = {
                mutablePair.update(projection(rows.next()).copy(), null)
              }
            }
          })
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes: Seq[SortOrder] = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        val part = new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
        Some(part)
      case HashPartitioning(_, n) =>
        Some(new PartitionIdPassthrough(n))
      case _ => None
    }

    val inputTypes = new Array[DataType](outputAttributes.size)
    outputAttributes.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    // gen RoundRobin pid
    def getRoundRobinPartitionKey: (ColumnarBatch, Int) => IntVec = {
      // 随机数
      (columnarBatch: ColumnarBatch, numPartitions: Int) => {
        val pidArr = new Array[Int](columnarBatch.numRows())
        for (i <- 0 until columnarBatch.numRows()) {
          val partitionId = TaskContext.get().partitionId()
          val position = new XORShiftRandom(partitionId).nextInt(numPartitions)
          pidArr(i) = position
        }
        val vec = new IntVec(columnarBatch.numRows())
        vec.put(pidArr, 0, 0, pidArr.length)
        vec
      }
    }

    def addPidToColumnBatch(): (IntVec, ColumnarBatch) => (Int, ColumnarBatch) = (pidVec, cb) => {
      val pidVecTmp = new OmniColumnVector(cb.numRows(), IntegerType, false)
      pidVecTmp.setVec(pidVec)
      val newColumns = (pidVecTmp +: (0 until cb.numCols).map(cb.column)).toArray
      (0, new ColumnarBatch(newColumns, cb.numRows))
    }

    def computePartitionId(cbIter: Iterator[ColumnarBatch],
                           partitionKeyExtractor: InternalRow => Any): Iterator[(Int, ColumnarBatch)] = {
      val addPid2ColumnBatch = addPidToColumnBatch()
      cbIter.filter(cb => cb.numRows != 0 && cb.numCols != 0).map {
        cb =>
          var pidVec: IntVec = null
          try {
            val pidArr = new Array[Int](cb.numRows)
            (0 until cb.numRows).foreach { i =>
              val row = cb.getRow(i)
              val pid = part.get.getPartition(partitionKeyExtractor(row))
              pidArr(i) = pid
            }
            pidVec = new IntVec(cb.numRows)
            pidVec.put(pidArr, 0, 0, cb.numRows)

            addPid2ColumnBatch(pidVec, cb)
          } catch {
            case e: Exception =>
              if (pidVec != null) {
                pidVec.close()
              }
              throw e
          }
      }
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    def containsRollUp(expressions: Seq[Expression]) : Boolean = {
      expressions.exists{
        case attr: AttributeReference if rollupConst.equals(attr.name) => true
        case _ => false
      }
    }

    val rddWithPartitionId: RDD[Product2[Int, ColumnarBatch]] = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // 按随机数分区
        rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
          val getRoundRobinPid = getRoundRobinPartitionKey
          val addPid2ColumnBatch = addPidToColumnBatch()
          cbIter.map { cb =>
            var pidVec: IntVec = null
            try {
              pidVec = getRoundRobinPid(cb, numPartitions)
              addPid2ColumnBatch(pidVec, cb)
            } catch {
              case e: Exception =>
                if (pidVec != null) {
                  pidVec.close()
                }
                throw e
            }
          }
        }, isOrderSensitive = isOrderSensitive)
      case RangePartitioning(sortingExpressions, _) =>
        // 排序，按采样数据进行分区
        rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
          val partitionKeyExtractor: InternalRow => Any = {
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            row => projection(row)
          }
          val newIter = computePartitionId(cbIter, partitionKeyExtractor)
          newIter
        }, isOrderSensitive = isOrderSensitive)
      case h@HashPartitioning(expressions, numPartitions) =>
        //containsRollUp(expressions): Avoid data skew caused by rollup expressions.
        //expressions.length > 6: Avoid q11 data skew
        //expressions.length == 3: Avoid q28 data skew when the resin rule is enabled.
        if (containsRollUp(expressions) || expressions.length > 6 || expressions.length == 3) {
          rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
            val partitionKeyExtractor: InternalRow => Any = {
              val projection =
                UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
              row => projection(row).getInt(0)
            }
            val newIter = computePartitionId(cbIter, partitionKeyExtractor)
            newIter
          }, isOrderSensitive = isOrderSensitive)
        } else {
          rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
              val addPid2ColumnBatch = addPidToColumnBatch()
              // omni project
              val genHashExpression = genHashExpr()
              val omniExpr: String = genHashExpression(expressions, numPartitions, defaultMm3HashSeed, outputAttributes)
              val factory = new OmniProjectOperatorFactory(Array(omniExpr), inputTypes, 1,
                new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
              val op = factory.createOperator()
              // close operator
              addLeakSafeTaskCompletionListener[Unit](_ => {
                op.close()
              })

              cbIter.map { cb =>
                var pidVec: IntVec = null
                try {
                  val vecs = transColBatchToOmniVecs(cb, true)
                  op.addInput(new VecBatch(vecs, cb.numRows()))
                  val res = op.getOutput
                  if (res.hasNext) {
                    val retBatch = res.next()
                    pidVec = retBatch.getVectors()(0).asInstanceOf[IntVec]
                    // close return VecBatch
                    retBatch.close()
                    addPid2ColumnBatch(pidVec, cb)
                  } else {
                    throw new Exception("Empty Project Operator Result...")
                  }
                } catch {
                  case e: Exception =>
                    if (pidVec != null) {
                      pidVec.close()
                    }
                    throw e
                }
              }
          }, isOrderSensitive = isOrderSensitive)
      }
      case SinglePartition =>
        rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
          cbIter.map { cb => (0, cb) }
        }, isOrderSensitive = isOrderSensitive)
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    val numCols = outputAttributes.size
    val intputTypeArr: Seq[DataType] = outputAttributes.map { attr =>
      sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val intputTypes = DataTypeSerializer.serialize(intputTypeArr.toArray)

    val partitionInfo: PartitionInfo = newPartitioning match {
      case SinglePartition =>
        new PartitionInfo("single", 1, numCols, intputTypes)
      case RoundRobinPartitioning(numPartitions) =>
        new PartitionInfo("rr", numPartitions, numCols, intputTypes)
      case HashPartitioning(expressions, numPartitions) =>
        new PartitionInfo("hash", numPartitions, numCols, intputTypes)
      case RangePartitioning(ordering, numPartitions) =>
        new PartitionInfo("range", numPartitions, numCols, intputTypes)
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rddWithPartitionId,
      new PartitionIdPassthrough(newPartitioning.numPartitions),
      serializer,
      handleRow = handleRow,
      shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
      partitionInfo = partitionInfo,
      dataSize = dataSize,
      bytesSpilled = bytesSpilled,
      numInputRows = numInputRows,
      splitTime = splitTime,
      spillTime = spillTime)
  }

  // gen hash partition expression
  def genHashExpr(): (Seq[Expression], Int, Int, Seq[Attribute]) => String = {
    (expressions: Seq[Expression], numPartitions: Int, seed: Int, outputAttributes: Seq[Attribute]) => {
      val exprIdMap = getExprIdMap(outputAttributes)
      val EXP_JSON_FORMATER1 = ("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"%s\",\"arguments\":[" +
        "%s,{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":%d}]}")
      val EXP_JSON_FORMATER2 = ("{\"exprType\": \"FUNCTION\",\"returnType\":1,\"function_name\":\"%s\", \"arguments\": [%s,%s] }")
      var omniExpr: String = ""
      expressions.foreach { expr =>
        val colExpr = rewriteToOmniJsonExpressionLiteral(expr, exprIdMap)
        if (omniExpr.isEmpty) {
          omniExpr = EXP_JSON_FORMATER1.format("mm3hash", colExpr, seed)
        } else {
          omniExpr = EXP_JSON_FORMATER2.format("mm3hash", colExpr, omniExpr)
        }
      }
      omniExpr = EXP_JSON_FORMATER1.format("pmod", omniExpr, numPartitions)
      logDebug(s"hash omni expression: $omniExpr")
      omniExpr
    }
  }

}
