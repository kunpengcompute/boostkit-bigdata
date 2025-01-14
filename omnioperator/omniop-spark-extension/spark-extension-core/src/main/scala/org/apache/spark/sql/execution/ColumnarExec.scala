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

import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.util.{BroadcastUtils, SparkMemoryUtils}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OmniColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import nova.hetu.omniruntime.vector.Vec

/**
 * Provides an optimized set of APIs to append row based data to an array of
 * [[WritableColumnVector]].
 */
private[execution] class OmniRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => OmniRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: InternalRow, vectors: Array[WritableColumnVector]): Unit = {
    var idx = 0
    while (idx < row.numFields) {
      converters(idx).append(row, idx, vectors(idx))
      idx += 1
    }
  }
}

/**
 * Provides an optimized set of APIs to extract a column from a row and append it to a
 * [[WritableColumnVector]].
 */
private object OmniRowToColumnConverter {
  SparkMemoryUtils.init()

  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit
  }

  private final case class BasicNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendNull
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    val core = dataType match {
      case BinaryType => BinaryConverter
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType | DateType => IntConverter
      case LongType | TimestampType => LongConverter
      case DoubleType => DoubleConverter
      case StringType => StringConverter
      case CalendarIntervalType => CalendarConverter
      case dt: DecimalType => DecimalConverter(dt)
      case unknown => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }

    if (nullable) {
      dataType match {
        case _ => new BasicNullableTypeConverter(core)
      }
    } else {
      core
    }
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val bytes = row.getBinary(column)
      cv.appendByteArray(bytes, 0, bytes.length)
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendBoolean(row.getBoolean(column))
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendByte(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendShort(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendInt(row.getInt(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendLong(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendDouble(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val data = row.getUTF8String(column).getBytes
      cv.asInstanceOf[OmniColumnVector].appendString(data.length, data, 0)
    }
  }

  private object CalendarConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val c = row.getInterval(column)
      cv.appendStruct(false)
      cv.getChild(0).appendInt(c.months)
      cv.getChild(1).appendInt(c.days)
      cv.getChild(2).appendLong(c.microseconds)
    }
  }

  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val d = row.getDecimal(column, dt.precision, dt.scale)
      if (DecimalType.is64BitDecimalType(dt)) {
        cv.appendLong(d.toUnscaledLong)
      } else {
        cv.asInstanceOf[OmniColumnVector].appendDecimal(d)
      }
    }
  }
}

/**
 * A trait that is used as a tag to indicate a transition from rows to columns. This allows plugins
 * to replace the current [[RowToColumnarExec]] with an optimized version and still have operations
 * that walk a spark plan looking for this type of transition properly match it.
 */
trait RowToColumnarTransition extends UnaryExecNode

/**
 * Provides a common executor to translate an [[RDD]] of [[InternalRow]] into an [[RDD]] of
 * [[ColumnarBatch]]. This is inserted whenever such a transition is determined to be needed.
 *
 * This is similar to some of the code in ArrowConverters.scala and
 * [[org.apache.spark.sql.execution.arrow.ArrowWriter]]. That code is more specialized
 * to convert [[InternalRow]] to Arrow formatted data, but in the future if we make
 * [[OffHeapColumnVector]] internally Arrow formatted we may be able to replace much of that code.
 *
 * This is also similar to
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate()]] and
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.toBatch()]] toBatch is only ever
 * called from tests and can probably be removed, but populate is used by both Orc and Parquet
 * to initialize partition and missing columns. There is some chance that we could replace
 * populate with [[RowToColumnConverter]], but the performance requirements are different and it
 * would only be to reduce code.
 */

case class RowToOmniColumnarExec(child: SparkPlan) extends RowToColumnarTransition {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val rowToOmniColumnarTime = longMetric("rowToOmniColumnarTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    val enableOffHeapColumnVector = session.sqlContext.conf.offHeapColumnVectorEnabled
    val localSchema = this.schema
    val relation = child.executeBroadcast()
    val mode = BroadcastUtils.getBroadCastMode(outputPartitioning)
    val broadcast: Broadcast[T] = BroadcastUtils.sparkToOmniUnsafe(sparkContext,
      mode,
      relation,
      logError,
      InternalRowToColumnarBatch.convert(
        enableOffHeapColumnVector,
        numInputRows,
        numOutputBatches,
        rowToOmniColumnarTime,
        numRows,
        localSchema,
        _)
    )
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, Seq(numInputRows, numOutputBatches, rowToOmniColumnarTime))
    broadcast
  }

  override def nodeName: String = "RowToOmniColumnar"

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "rowToOmniColumnarTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in row to OmniColumnar")
  )

  override protected def withNewChildInternal(newChild: SparkPlan): RowToOmniColumnarExec =
    copy(child = newChild)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val enableOffHeapColumnVector = session.sqlContext.conf.offHeapColumnVectorEnabled
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val rowToOmniColumnarTime = longMetric("rowToOmniColumnarTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    child.execute().mapPartitionsInternal { rowIterator =>
      InternalRowToColumnarBatch.convert(enableOffHeapColumnVector, numInputRows, numOutputBatches, rowToOmniColumnarTime, numRows, localSchema, rowIterator)
    }
  }

}


case class OmniColumnarToRowExec(child: SparkPlan,
                                 mayPartialFetch: Boolean = true) extends ColumnarToRowTransition {
  override def nodeName: String = "OmniColumnarToRow"

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "omniColumnarToRowTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omniColumnar to row")
  )

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |$simpleStringWithNodeId
       |${ExplainUtils.generateFieldString("mayPartialFetch", String.valueOf(mayPartialFetch))}
       |""".stripMargin
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val omniColumnarToRowTime = longMetric("omniColumnarToRowTime")
    val mode = BroadcastUtils.getBroadCastMode(outputPartitioning)
    val relation = child.executeBroadcast()
    val broadcast: Broadcast[T] = BroadcastUtils.omniToSparkUnsafe(sparkContext,
      mode,
      relation,
      StructType(output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))),
      ColumnarBatchToInternalRow.convert(output, _, numOutputRows, numInputBatches, omniColumnarToRowTime)
    )
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, Seq(numOutputRows, numInputBatches, omniColumnarToRowTime))
    broadcast
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val omniColumnarToRowTime = longMetric("omniColumnarToRowTime")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    child.executeColumnar().mapPartitionsInternal { batches =>
      ColumnarBatchToInternalRow.convert(localOutput, batches, numOutputRows, numInputBatches, omniColumnarToRowTime, mayPartialFetch)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan):
  OmniColumnarToRowExec = copy(child = newChild)
}

object InternalRowToColumnarBatch {

  def convert(enableOffHeapColumnVector: Boolean,
              numInputRows: SQLMetric,
              numOutputBatches: SQLMetric,
              rowToOmniColumnarTime: SQLMetric,
              numRows: Int, localSchema: StructType,
              rowIterator: Iterator[InternalRow]): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      new Iterator[ColumnarBatch] {
        private val converters = new OmniRowToColumnConverter(localSchema)

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          val startTime = System.nanoTime()
          val vectors: Seq[WritableColumnVector] = OmniColumnVector.allocateColumns(numRows,
            localSchema, true)
          val cb: ColumnarBatch = new ColumnarBatch(vectors.toArray)
          cb.setNumRows(0)
          vectors.foreach(_.reset())
          var rowCount = 0
          while (rowCount < numRows && rowIterator.hasNext) {
            val row = rowIterator.next()
            converters.convert(row, vectors.toArray)
            rowCount += 1
          }
          if (!enableOffHeapColumnVector) {
            vectors.foreach { v =>
              v.asInstanceOf[OmniColumnVector].getVec.setSize(rowCount)
            }
          }
          cb.setNumRows(rowCount)
          numInputRows += rowCount
          numOutputBatches += 1
          rowToOmniColumnarTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)
          cb
        }
      }
    } else {
      Iterator.empty
    }
  }

}

object ColumnarBatchToInternalRow {

  def convert(output: Seq[Attribute], batches: Iterator[ColumnarBatch],
              numOutputRows: SQLMetric, numInputBatches: SQLMetric,
              rowToOmniColumnarTime: SQLMetric,
              mayPartialFetch: Boolean = true): Iterator[InternalRow] = {
    val startTime = System.nanoTime()
    val toUnsafe = UnsafeProjection.create(output, output)

    val batchIter = batches.flatMap { batch =>

      // toClosedVecs closed case: [Deprcated]
      // 1) all rows of batch fetched and closed
      // 2) only fetch Partial rows(eg: top-n, limit-n), closed at task CompletionListener callback
      val toClosedVecs = new ListBuffer[Vec]
      for (i <- 0 until batch.numCols()) {
        batch.column(i) match {
          case vector: OmniColumnVector =>
            toClosedVecs.append(vector.getVec)
          case _ =>
        }
      }

      numInputBatches += 1
      val iter = batch.rowIterator().asScala.map(toUnsafe)
      rowToOmniColumnarTime += NANOSECONDS.toMillis(System.nanoTime() - startTime)

      new Iterator[InternalRow] {
        val numOutputRowsMetric: SQLMetric = numOutputRows


        SparkMemoryUtils.addLeakSafeTaskCompletionListener { _ =>
          toClosedVecs.foreach { vec =>
            vec.close()
          }
        }

        override def hasNext: Boolean = {
          val has = iter.hasNext
          // fetch all rows
          if (!has) {
            toClosedVecs.foreach { vec =>
              vec.close()
              toClosedVecs.remove(toClosedVecs.indexOf(vec))
            }
          }
          has
        }

        override def next(): InternalRow = {
          numOutputRowsMetric += 1
          iter.next()
        }
      }
    }
    batchIter
  }
}