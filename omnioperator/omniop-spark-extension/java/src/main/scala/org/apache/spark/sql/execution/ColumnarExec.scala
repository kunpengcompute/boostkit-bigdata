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

import nova.hetu.omniruntime.vector.Vec

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OmniColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Holds a user defined rule that can be used to inject columnar implementations of various
 * operators in the plan. The [[preColumnarTransitions]] [[Rule]] can be used to replace
 * [[SparkPlan]] instances with versions that support a columnar implementation. After this
 * Spark will insert any transitions necessary. This includes transitions from row to columnar
 * [[RowToColumnarExec]] and from columnar to row [[ColumnarToRowExec]]. At this point the
 * [[postColumnarTransitions]] [[Rule]] is called to allow replacing any of the implementations
 * of the transitions or doing cleanup of the plan, like inserting stages to build larger batches
 * for more efficient processing, or stages that transition the data to/from an accelerator's
 * memory.
 */
class ColumnarRule {
  def preColumnarTransitions: Rule[SparkPlan] = plan => plan
  def postColumnarTransitions: Rule[SparkPlan] = plan => plan
}

/**
 * A trait that is used as a tag to indicate a transition from columns to rows. This allows plugins
 * to replace the current [[ColumnarToRowExec]] with an optimized version and still have operations
 * that walk a spark plan looking for this type of transition properly match it.
 */
trait ColumnarToRowTransition extends UnaryExecNode


/**
 * Provides an optimized set of APIs to append row based data to an array of
 * [[WritableColumnVector]].
 */
private[execution] class RowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => RowToColumnConverter.getConverterForType(f.dataType, f.nullable)
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
private object RowToColumnConverter {
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
    child.doExecuteBroadcast()
  }

  override def nodeName: String = "RowToOmniColumnar"

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches")
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val enableOffHeapColumnVector = sqlContext.conf.offHeapColumnVectorEnabled
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    child.execute().mapPartitionsInternal { rowIterator =>
      if (rowIterator.hasNext) {
        new Iterator[ColumnarBatch] {
          private val converters = new RowToColumnConverter(localSchema)

          override def hasNext: Boolean = {
            rowIterator.hasNext
          }

          override def next(): ColumnarBatch = {
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
            cb
          }
        }
      } else {
        Iterator.empty
      }
    }
  }
}


case class OmniColumnarToRowExec(child: SparkPlan) extends ColumnarToRowTransition {
  assert(child.supportsColumnar)

  override def nodeName: String = "OmniColumnarToRow"

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches")
  )

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    child.executeColumnar().mapPartitionsInternal { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      val vecsTmp = new ListBuffer[Vec]

      val batchIter = batches.flatMap { batch =>
        // store vec since tablescan reuse batch
        for (i <- 0 until batch.numCols()) {
          batch.column(i) match {
            case vector: OmniColumnVector =>
              vecsTmp.append(vector.getVec)
            case _ =>
          }
        }
        numInputBatches += 1
        numOutputRows += batch.numRows()
        batch.rowIterator().asScala.map(toUnsafe)
      }

      SparkMemoryUtils.addLeakSafeTaskCompletionListener { _ =>
        vecsTmp.foreach {vec =>
          vec.close()
        }
      }
      batchIter
    }
  }
}
