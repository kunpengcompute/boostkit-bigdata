package org.apache.spark.sql.execution.command

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Cast, Ceil, Coalesce, CreateNamedStruct, CreateStruct, Expression, Least, Length, Literal, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, ApproxCountDistinctForIntervals, ApproximatePercentile, Average, Count, HyperLogLogPlusPlus, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStat, Histogram, HistogramBin, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegralType, LongType, StringType, TimestampType}

import scala.collection.mutable

object NdpCommandUtils extends Logging {

  private[sql] def computeColumnStats(
                                       sparkSession: SparkSession,
                                       relation: LogicalPlan,
                                       columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    val conf = sparkSession.sessionState.conf

    // Collect statistics per column.
    // If no histogram is required, we run a job to compute basic column stats such as
    // min, max, ndv, etc. Otherwise, besides basic column stats, histogram will also be
    // generated. Currently we only support equi-height histogram.
    // To generate an equi-height histogram, we need two jobs:
    // 1. compute percentiles p(0), p(1/n) ... p((n-1)/n), p(1).
    // 2. use the percentiles as value intervals of bins, e.g. [p(0), p(1/n)],
    // [p(1/n), p(2/n)], ..., [p((n-1)/n), p(1)], and then count ndv in each bin.
    // Basic column stats will be computed together in the second job.
    val attributePercentiles = computePercentiles(columns, sparkSession, relation)

    // The first element in the result will be the overall row count, the following elements
    // will be structs containing all column stats.
    // The layout of each struct follows the layout of the ColumnStats.
    val expressions = Count(Literal(1)).toAggregateExpression() +:
      columns.map(statExprs(_, conf, attributePercentiles))

    val namedExpressions = expressions.map(e => Alias(e, e.toString)())
    val statsRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExpressions, relation))
      .executedPlan.executeTake(1).head

    val rowCount = statsRow.getLong(0)
    val columnStats = columns.zipWithIndex.map { case (attr, i) =>
      // according to `statExprs`, the stats struct always have 7 fields.
      (attr, rowToColumnStat(statsRow.getStruct(i + 1, 7), attr, rowCount,
        attributePercentiles.get(attr)))
    }.toMap
    (rowCount, columnStats)
  }

  private def computePercentiles(
                                  attributesToAnalyze: Seq[Attribute],
                                  sparkSession: SparkSession,
                                  relation: LogicalPlan): AttributeMap[ArrayData] = {
    val conf = sparkSession.sessionState.conf
    val attrsToGenHistogram = if (conf.histogramEnabled) {
      attributesToAnalyze.filter(a => supportsHistogram(a.dataType))
    } else {
      Nil
    }
    val attributePercentiles = mutable.HashMap[Attribute, ArrayData]()
    if (attrsToGenHistogram.nonEmpty) {
      val percentiles = (0 to conf.histogramNumBins)
        .map(i => i.toDouble / conf.histogramNumBins).toArray

      val namedExprs = attrsToGenHistogram.map { attr =>
        val aggFunc =
          new ApproximatePercentile(attr,
            Literal(new GenericArrayData(percentiles), ArrayType(DoubleType, false)),
            Literal(conf.percentileAccuracy))
        val expr = aggFunc.toAggregateExpression()
        Alias(expr, expr.toString)()
      }

      val percentilesRow = new QueryExecution(sparkSession, Aggregate(Nil, namedExprs, relation))
        .executedPlan.executeTake(1).head
      attrsToGenHistogram.zipWithIndex.foreach { case (attr, i) =>
        val percentiles = percentilesRow.getArray(i)
        // When there is no non-null value, `percentiles` is null. In such case, there is no
        // need to generate histogram.
        if (percentiles != null) {
          attributePercentiles += attr -> percentiles
        }
      }
    }
    AttributeMap(attributePercentiles.toSeq)
  }

  private def statExprs(
                         col: Attribute,
                         conf: SQLConf,
                         colPercentiles: AttributeMap[ArrayData]): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val one = Literal(1.toLong, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = countDistinct(col.name).expr
    val numNulls = Subtract(Count(one), numNonNulls)
    val defaultSize = Literal(col.dataType.defaultSize.toLong, LongType)
    val nullArray = Literal(null, ArrayType(LongType))

    def fixedLenTypeStruct: CreateNamedStruct = {
      val genHistogram =
        supportsHistogram(col.dataType) && colPercentiles.contains(col)
      val intervalNdvsExpr = if (genHistogram) {
        ApproxCountDistinctForIntervals(col,
          Literal(colPercentiles(col), ArrayType(col.dataType)), conf.ndvMaxError)
      } else {
        nullArray
      }
      // For fixed width types, avg size should be the same as max size.
      struct(ndv, Cast(Min(col), col.dataType), Cast(Max(col), col.dataType), numNulls,
        defaultSize, defaultSize, intervalNdvsExpr)
    }

    col.dataType match {
      case _: IntegralType => fixedLenTypeStruct
      case _: DecimalType => fixedLenTypeStruct
      case DoubleType | FloatType => fixedLenTypeStruct
      case BooleanType => fixedLenTypeStruct
      case DateType => fixedLenTypeStruct
      case TimestampType => fixedLenTypeStruct
      case StringType => fixedLenTypeStruct
      case BinaryType =>
        // For binary type, we don't compute min, max or histogram
        val nullLit = Literal(null, col.dataType)
        struct(
          ndv, nullLit, nullLit, numNulls,
          // Set avg/max size to default size if all the values are null or there is no value.
          Coalesce(Seq(Ceil(Average(Length(col))), defaultSize)),
          Coalesce(Seq(Cast(Max(Length(col)), LongType), defaultSize)),
          nullArray)
      case _ =>
        throw QueryCompilationErrors.analyzingColumnStatisticsNotSupportedForColumnTypeError(
          col.name, col.dataType)
    }
  }

  private def supportsHistogram(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case DateType => true
    case TimestampType => true
    case _ => false
  }

  private def rowToColumnStat(
                               row: InternalRow,
                               attr: Attribute,
                               rowCount: Long,
                               percentiles: Option[ArrayData]): ColumnStat = {
    // The first 6 fields are basic column stats, the 7th is ndvs for histogram bins.
    val cs = ColumnStat(
      distinctCount = Option(BigInt(row.getLong(0))),
      // for string/binary min/max, get should return null
      min = Option(row.get(1, attr.dataType)),
      max = Option(row.get(2, attr.dataType)),
      nullCount = Option(BigInt(row.getLong(3))),
      avgLen = Option(row.getLong(4)),
      maxLen = Option(row.getLong(5))
    )
    if (row.isNullAt(6) || cs.nullCount.isEmpty) {
      cs
    } else {
      val ndvs = row.getArray(6).toLongArray()
      assert(percentiles.get.numElements() == ndvs.length + 1)
      val endpoints = percentiles.get.toArray[Any](attr.dataType).map(_.toString.toDouble)
      // Construct equi-height histogram
      val bins = ndvs.zipWithIndex.map { case (ndv, i) =>
        HistogramBin(endpoints(i), endpoints(i + 1), ndv)
      }
      val nonNullRows = rowCount - cs.nullCount.get
      val histogram = Histogram(nonNullRows.toDouble / ndvs.length, bins)
      cs.copy(histogram = Some(histogram))
    }
  }
}