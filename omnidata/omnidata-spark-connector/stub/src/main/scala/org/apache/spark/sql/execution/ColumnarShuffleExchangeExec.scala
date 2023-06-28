package org.apache.spark.sql.execution

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike, ShuffleOrigin}

import scala.concurrent.Future

case class ColumnarShuffleExchangeExec(
                                        override val outputPartitioning: Partitioning,
                                        child: SparkPlan,
                                        shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends ShuffleExchangeLike {
  override def numMappers: Int = ???

  override def numPartitions: Int = ???

  override protected def mapOutputStatisticsFuture: Future[MapOutputStatistics] = ???

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] = ???

  override def runtimeStatistics: Statistics = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???

  override protected def doExecute(): RDD[InternalRow] = ???
}
