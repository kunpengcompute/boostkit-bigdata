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

import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{Partition, SparkContext, TaskContext, broadcast}


private final case class BroadcastColumnarRDDPartition(index: Int) extends Partition

case class BroadcastColumnarRDD(
    @transient private val sc: SparkContext,
    metrics: Map[String, SQLMetric],
    numPartitioning: Int,
    inputByteBuf: broadcast.Broadcast[ColumnarHashedRelation],
    localSchema: StructType)
    extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    (0 until numPartitioning).map { index => new BroadcastColumnarRDDPartition(index) }.toArray
  }

  private def vecBatchToColumnarBatch(vecBatch: VecBatch): ColumnarBatch = {
    val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
      vecBatch.getRowCount, localSchema, false)
    vectors.zipWithIndex.foreach { case (vector, i) =>
      vector.reset()
      vector.setVec(vecBatch.getVectors()(i))
    }
    vecBatch.close()
    new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    // val relation = inputByteBuf.value.asReadOnlyCopy
    // new CloseableColumnBatchIterator(relation.getColumnarBatchAsIter)
    val deserializer = VecBatchSerializerFactory.create()
    new Iterator[ColumnarBatch] {
      var idx = 0
      val total_len = inputByteBuf.value.buildData.length

      override def hasNext: Boolean = idx < total_len

      override def next(): ColumnarBatch = {
        val batch: VecBatch = deserializer.deserialize(inputByteBuf.value.buildData(idx))
        idx += 1
        vecBatchToColumnarBatch(batch)
      }
    }
  }
}
