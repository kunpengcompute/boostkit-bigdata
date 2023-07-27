package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Histogram, HistogramSerializer}
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.types._

import java.time.ZoneOffset
import scala.util.control.NonFatal

case class CatalogColumnStat(
                              distinctCount: Option[BigInt] = None,
                              min: Option[String] = None,
                              max: Option[String] = None,
                              nullCount: Option[BigInt] = None,
                              avgLen: Option[Long] = None,
                              maxLen: Option[Long] = None,
                              histogram: Option[Histogram] = None,
                              version: Int = CatalogColumnStat.VERSION) {

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the column and name of the field (e.g. "colName.distinctCount"),
   * and the value is the string representation for the value.
   * min/max values are stored as Strings. They can be deserialized using
   * [[CatalogColumnStat.fromExternalString]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   * Any of the fields that are null (None) won't appear in the map.
   */
  def toMap(colName: String): Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(s"${colName}.${CatalogColumnStat.KEY_VERSION}", CatalogColumnStat.VERSION.toString)
    distinctCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_DISTINCT_COUNT}", v.toString)
    }
    nullCount.foreach { v =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_NULL_COUNT}", v.toString)
    }
    avgLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_AVG_LEN}", v.toString) }
    maxLen.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_LEN}", v.toString) }
    min.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MIN_VALUE}", v) }
    max.foreach { v => map.put(s"${colName}.${CatalogColumnStat.KEY_MAX_VALUE}", v) }
    histogram.foreach { h =>
      map.put(s"${colName}.${CatalogColumnStat.KEY_HISTOGRAM}", HistogramSerializer.serialize(h))
    }
    map.toMap
  }

  /** Convert [[CatalogColumnStat]] to [[ColumnStat]]. */
  def toPlanStat(
                  colName: String,
                  dataType: DataType): ColumnStat =
    ColumnStat(
      distinctCount = distinctCount,
      min = min.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      max = max.map(CatalogColumnStat.fromExternalString(_, colName, dataType, version)),
      nullCount = nullCount,
      avgLen = avgLen,
      maxLen = maxLen,
      histogram = histogram,
      version = version)
}

object CatalogColumnStat extends Logging {

  // List of string keys used to serialize CatalogColumnStat
  val KEY_VERSION = "version"
  private val KEY_DISTINCT_COUNT = "distinctCount"
  private val KEY_MIN_VALUE = "min"
  private val KEY_MAX_VALUE = "max"
  private val KEY_NULL_COUNT = "nullCount"
  private val KEY_AVG_LEN = "avgLen"
  private val KEY_MAX_LEN = "maxLen"
  private val KEY_HISTOGRAM = "histogram"

  val VERSION = 2

  private def getTimestampFormatter(isParsing: Boolean): TimestampFormatter = {
    TimestampFormatter(
      format = "yyyy-MM-dd HH:mm:ss.SSSSSS",
      zoneId = ZoneOffset.UTC,
      isParsing = isParsing)
  }

  /**
   * Converts from string representation of data type to the corresponding Catalyst data type.
   */
  def fromExternalString(s: String, name: String, dataType: DataType, version: Int): Any = {
    dataType match {
      case BooleanType => s.toBoolean
      case DateType if version == 1 => DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(s))
      case DateType => DateFormatter(ZoneOffset.UTC).parse(s)
      case TimestampType if version == 1 =>
        DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
      case TimestampType => getTimestampFormatter(isParsing = true).parse(s)
      case ByteType => s.toByte
      case ShortType => s.toShort
      case IntegerType => s.toInt
      case LongType => s.toLong
      case FloatType => s.toFloat
      case DoubleType => s.toDouble
      case _: DecimalType => Decimal(s)
      case StringType => s
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case BinaryType => null
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column $name of data type: $dataType.")
    }
  }

  /**
   * Converts the given value from Catalyst data type to string representation of external
   * data type.
   */
  def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateFormatter(ZoneOffset.UTC).format(v.asInstanceOf[Int])
      case TimestampType => getTimestampFormatter(isParsing = false).format(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType | StringType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case _ =>
        throw new AnalysisException("Column statistics serialization is not supported for " +
          s"column $colName of data type: $dataType.")
    }
    externalValue.toString
  }


  /**
   * Creates a [[CatalogColumnStat]] object from the given map.
   * This is used to deserialize column stats from some external storage.
   * The serialization side is defined in [[CatalogColumnStat.toMap]].
   */
  def fromMap(
               table: String,
               colName: String,
               map: Map[String, String]): Option[CatalogColumnStat] = {

    try {
      Some(CatalogColumnStat(
        distinctCount = map.get(s"${colName}.${KEY_DISTINCT_COUNT}").map(v => BigInt(v.toLong)),
        min = map.get(s"${colName}.${KEY_MIN_VALUE}"),
        max = map.get(s"${colName}.${KEY_MAX_VALUE}"),
        nullCount = map.get(s"${colName}.${KEY_NULL_COUNT}").map(v => BigInt(v.toLong)),
        avgLen = map.get(s"${colName}.${KEY_AVG_LEN}").map(_.toLong),
        maxLen = map.get(s"${colName}.${KEY_MAX_LEN}").map(_.toLong),
        histogram = map.get(s"${colName}.${KEY_HISTOGRAM}").map(HistogramSerializer.deserialize),
        version = map(s"${colName}.${KEY_VERSION}").toInt
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to parse column statistics for column ${colName} in table $table", e)
        None
    }
  }
}

