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

import java.util.Optional
import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor

import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory
import nova.hetu.omniruntime.vector.{Vec, VecBatch}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil._
import nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniHashBuilderWithExprOperatorFactory, OmniLookupJoinWithExprOperatorFactory}
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.{OmniOrcFileFormat, OrcFileFormat}
import org.apache.spark.sql.execution.joins.ColumnarBroadcastHashJoinExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet



abstract class BaseColumnarFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends DataSourceScanExec {

  override lazy val supportsColumnar: Boolean = true

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  /**
   * [[partitionFilters]] can contain subqueries whose results are available only at runtime so
   * accessing [[selectedPartitions]] should be guarded by this method during planning
   */
  private def hasPartitionsAvailableAtRunTime: Boolean = {
    partitionFilters.exists(ExecSubqueryExpression.hasSubquery)
  }

  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  // exposed for testing
  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled && relation.bucketSpec.isDefined
      && !disableBucketedScan) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      // For bucketed columns:
      // -----------------------
      // `HashPartitioning` would be used only when:
      // 1. ALL the bucketing columns are being read from the table
      //
      // For sorted columns:
      // ---------------------
      // Sort ordering should be used when ALL these criteria's match:
      // 1. `HashPartitioning` is being used
      // 2. A prefix (or all) of the sort columns are being read from the table.
      //
      // Sort ordering would be over the prefix subset of `sort columns` being read
      // from the table.
      // e.g.
      // Assume (col0, col2, col3) are the columns read from the table
      // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
      // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
      // above
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      val numPartitions = optionalNumCoalescedBuckets.getOrElse(spec.numBuckets)
      val partitioning = HashPartitioning(bucketColumns, numPartitions)
      val sortColumns =
        spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)
      val shouldCalculateSortOrder =
        conf.getConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING) &&
          sortColumns.nonEmpty &&
          !hasPartitionsAvailableAtRunTime

      val sortOrder = if (shouldCalculateSortOrder) {
        // In case of bucketing, its possible to have multiple files belonging to the
        // same bucket in a given relation. Each of these files are locally sorted
        // but those files combined together are not globally sorted. Given that,
        // the RDD partition will not be sorted even if the relation has sort columns set
        // Current solution is to check if all the buckets have a single file in it

        val files = selectedPartitions.flatMap(partition => partition.files)
        val bucketToFilesGrouping =
          files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
        val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

        // SPARK-24528 Sort order is currently ignored if buckets are coalesced.
        if (singleFilePartitions && optionalNumCoalescedBuckets.isEmpty) {
          // Currently Spark does not support writing columns sorting in descending order
          // so using Ascending order. This can be fixed in future
          sortColumns.map(attribute => SortOrder(attribute, Ascending))
        } else {
          Nil
        }
      } else {
        Nil
      }
      (partitioning, sortOrder)
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  override protected def metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")

    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)

    // (SPARK-32986): Add bucketed scan info in explain output of FileSourceScanExec
    if (bucketedScan) {
      relation.bucketSpec.map { spec =>
        val numSelectedBuckets = optionalBucketSet.map { b =>
          b.cardinality()
        } getOrElse {
          spec.numBuckets
        }
        metadata + ("SelectedBucketsCount" ->
          (s"$numSelectedBuckets out of ${spec.numBuckets}" +
            optionalNumCoalescedBuckets.map { b => s" (Coalesced to $b)" }.getOrElse("")))
      } getOrElse {
        metadata
      }
    } else {
      metadata
    }
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLocation = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val fileFormat: FileFormat = relation.fileFormat match {
      case orcFormat: OrcFileFormat =>
        new OmniOrcFileFormat()
      case _ =>
        throw new UnsupportedOperationException("Unsupported FileFormat!")
    }
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions,
        relation)
    } else {
      createNonBucketedReadRDD(readFile, dynamicallySelectedPartitions, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  protected lazy val staticMetrics = if (partitionFilters.exists(isDynamicPruningFilter)) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchemaOption.isDefined) {
      driverMetrics("numPartitions") = partitions.length
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs")
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ {
    if (relation.partitionSchemaOption.isDefined) {
      Map(
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  } ++ staticMetrics

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  def buildCheck(): Unit = {
    output.zipWithIndex.foreach {
      case (attr, i) =>
        sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          numOutputVecBatchs += 1
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "ColumnarFile"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    // (SPARK-32985): Decouple bucket filter pruning and bucketed table scan
    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        FilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  protected def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  def genAggOutput(agg: HashAggregateExec) = {
    val attrAggExpsIdMap = getExprIdMap(agg.child.output)
    val omniGroupByChanel = agg.groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, attrAggExpsIdMap)).toArray

    var omniOutputExressionOrder: Map[ExprId, Int] = Map()
    var aggIndexOffset = 0
    agg.groupingExpressions.zipWithIndex.foreach { case (exp, index) =>
      omniOutputExressionOrder += (exp.exprId -> (index + aggIndexOffset))
    }
    aggIndexOffset += agg.groupingExpressions.size

    val omniAggInputRaws = new Array[Boolean](agg.aggregateExpressions.size)
    val omniAggOutputPartials =new Array[Boolean](agg.aggregateExpressions.size)
    val omniAggTypes = new Array[DataType](agg.aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType](agg.aggregateExpressions.size)
    val omniAggOutputTypes = new Array[Array[DataType]](agg.aggregateExpressions.size)
    val omniAggChannels = new Array[Array[String]](agg.aggregateExpressions.size)

    var omniAggindex = 0
    for (exp <- agg.aggregateExpressions) {
      if (exp.mode == Final) {
        throw new UnsupportedOperationException(s"Unsupported final aggregate expression in operator fusion, exp: $exp")
      } else if (exp.mode == Partial) {
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Average(_) | Max(_) | Count(_) | First(_, _) =>
            val aggExp = exp.aggregateFunction.children.head
            omniOutputExressionOrder += {
              exp.aggregateFunction.inputAggBufferAttributes.head.exprId ->
                (omniAggindex + aggIndexOffset)
            }
            omniAggTypes(omniAggindex) = sparkTypeToOmniType(aggExp.dataType)
            omniAggFunctionTypes(omniAggindex) = toOmniAggFunType(exp, true)
            omniAggOutputTypes(omniAggindex) =
              toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
            omniAggChannels(omniAggindex) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.children, attrAggExpsIdMap)
            omniAggInputRaws(omniAggindex) = true
            omniAggOutputPartials(omniAggindex) = true
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: $exp")
        }
      } else if (exp.mode == PartialMerge) {
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Average(_) | Max(_) | Count(_) | First(_, _) =>
            val aggExp = exp.aggregateFunction.children.head
            omniOutputExressionOrder += {
              exp.aggregateFunction.inputAggBufferAttributes.head.exprId ->
                (omniAggindex + aggIndexOffset)
            }
            omniAggTypes(omniAggindex) = sparkTypeToOmniType(aggExp.dataType)
            omniAggFunctionTypes(omniAggindex) = toOmniAggFunType(exp, true)
            omniAggOutputTypes(omniAggindex) =
              toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
            omniAggChannels(omniAggindex) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.inputAggBufferAttributes, attrAggExpsIdMap)
            omniAggInputRaws(omniAggindex) = false
            omniAggOutputPartials(omniAggindex) = true
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: $exp")
        }
      } else {
        throw new UnsupportedOperationException(s"Unsupported aggregate mode: $exp.mode")
      }
      omniAggindex += 1
    }

    var resultIdxToOmniResultIdxMap: Map[Int, Int] = Map()
    agg.resultExpressions.zipWithIndex.foreach { case (exp, index) =>
      if (omniOutputExressionOrder.contains(getRealExprId(exp))) {
        resultIdxToOmniResultIdxMap +=
          (index -> omniOutputExressionOrder(getRealExprId(exp)))
      }
    }

    val omniAggSourceTypes = new Array[DataType](agg.child.output.size)
    agg.child.output.zipWithIndex.foreach {
      case (attr, i) =>
        omniAggSourceTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    (omniGroupByChanel, omniAggChannels, omniAggSourceTypes, omniAggFunctionTypes, omniAggOutputTypes,
      omniAggInputRaws, omniAggOutputPartials, resultIdxToOmniResultIdxMap)
  }

  def genProjectOutput(project: ColumnarProjectExec) = {
    val omniAttrExpsIdMap = getExprIdMap(project.child.output)
    val omniInputTypes = project.child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = project.projectList.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    (omniExpressions, omniInputTypes)
  }

  def genJoinOutput(join: ColumnarBroadcastHashJoinExec) = {
    val buildTypes = new Array[DataType](join.getBuildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    join.getBuildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    // {0}, buildKeys: col1#12
    val buildOutputCols = join.getBuildOutput.indices.toArray // {0,1}
    val buildJoinColsExp = join.getBuildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(join.getBuildOutput.map(_.toAttribute)))
    }.toArray
    val relation = join.getBuildPlan.executeBroadcast[ColumnarHashedRelation]()

    val buildOutputTypes = buildTypes // {1,1}

    val probeTypes = new Array[DataType](join.getStreamedOutput.size) // {2,2},streamedOutput:col1#10,col2#11
    join.getStreamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = join.getStreamedOutput.indices.toArray// {0,1}
    val probeHashColsExp = join.getStreamedKeys.map {x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(join.getStreamedOutput.map(_.toAttribute)))
    }.toArray
    val filter: Option[String] = join.condition match {
      case Some(expr) =>
        Some(OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap((join.getStreamedOutput ++ join.getBuildOutput).map(_.toAttribute))))
      case _ => None
    }
    (buildTypes, buildJoinColsExp, filter, probeTypes, probeOutputCols,
      probeHashColsExp, buildOutputCols, buildOutputTypes, relation)
  }

  def genFilterOutput(cond: ColumnarFilterExec) = {
    val omniCondAttrExpsIdMap = getExprIdMap(cond.child.output)
    val omniCondInputTypes = cond.child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniCondExpressions = cond.child.output.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniCondAttrExpsIdMap)).toArray
    val conditionExpression = rewriteToOmniJsonExpressionLiteral(cond.condition, omniCondAttrExpsIdMap)
    (conditionExpression, omniCondInputTypes, omniCondExpressions)
  }

  def genJoinOutputWithReverse(join: ColumnarBroadcastHashJoinExec) = {
    val buildTypes = new Array[DataType](join.getBuildOutput.size) // {2,2}, buildOutput:col1#12,col2#13
    join.getBuildOutput.zipWithIndex.foreach { case (att, i) =>
      buildTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(att.dataType, att.metadata)
    }

    // {0}, buildKeys: col1#12
    val buildOutputCols = join.getBuildOutput.indices.toArray // {0,1}
    val buildJoinColsExp = join.getBuildKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(join.getBuildOutput.map(_.toAttribute)))
    }.toArray
    val relation = join.getBuildPlan.executeBroadcast[ColumnarHashedRelation]()

    val buildOutputTypes = buildTypes // {1,1}

    val probeTypes = new Array[DataType](join.getStreamedOutput.size) // {2,2},streamedOutput:col1#10,col2#11
    join.getStreamedOutput.zipWithIndex.foreach { case (attr, i) =>
      probeTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val probeOutputCols = join.getStreamedOutput.indices.toArray// {0,1}
    val probeHashColsExp = join.getStreamedKeys.map {x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(join.getStreamedOutput.map(_.toAttribute)))
    }.toArray
    val filter: Option[String] = join.condition match {
      case Some(expr) =>
        Some(OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap((join.getStreamedOutput ++ join.getBuildOutput).map(_.toAttribute))))
      case _ => None
    }

    val reverse = join.buildSide == BuildLeft
    var left = 0
    var leftLen = join.getStreamPlan.output.size
    var right = join.getStreamPlan.output.size
    var rightLen = join.output.size
    if (reverse) {
      left = join.getStreamPlan.output.size
      leftLen = join.output.size
      right = 0
      rightLen = join.getStreamPlan.output.size
    }
    (buildTypes, buildJoinColsExp, filter, probeTypes, probeOutputCols,
      probeHashColsExp, buildOutputCols, buildOutputTypes, relation, (left, leftLen, right, rightLen))
  }
}

case class ColumnarFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends BaseColumnarFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {
  override def doCanonicalize(): ColumnarFileSourceScanExec = {
    ColumnarFileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan)
  }
}

case class WrapperLeafExec() extends LeafExecNode {

  override def supportsColumnar: Boolean = true


  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override def output: Seq[Attribute] = Seq()
}

case class ColumnarMultipleOperatorExec(
    aggregate: HashAggregateExec,
    proj1: ColumnarProjectExec,
    join1: ColumnarBroadcastHashJoinExec,
    proj2: ColumnarProjectExec,
    join2: ColumnarBroadcastHashJoinExec,
    proj3: ColumnarProjectExec,
    join3: ColumnarBroadcastHashJoinExec,
    proj4: ColumnarProjectExec,
    join4: ColumnarBroadcastHashJoinExec,
    filter: ColumnarFilterExec,
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends BaseColumnarFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  protected override def doPrepare(): Unit = {
    super.doPrepare()
    join1.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
    join2.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
    join3.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
    join4.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "outputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "omniJitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs")
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ {
    if (relation.partitionSchemaOption.isDefined) {
      Map(
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  } ++ staticMetrics

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    val numInputRows = longMetric("numInputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniJitTime")
    val getOutputTime = longMetric("outputTime")

    val (omniGroupByChanel, omniAggChannels, omniAggSourceTypes, omniAggFunctionTypes, omniAggOutputTypes,
    omniAggInputRaw, omniAggOutputPartial, resultIdxToOmniResultIdxMap) = genAggOutput(aggregate)
    val (proj1OmniExpressions, proj1OmniInputTypes) = genProjectOutput(proj1)
    val (buildTypes1, buildJoinColsExp1, joinFilter1, probeTypes1, probeOutputCols1,
    probeHashColsExp1, buildOutputCols1, buildOutputTypes1, relation1) = genJoinOutput(join1)
    val (proj2OmniExpressions, proj2OmniInputTypes) = genProjectOutput(proj2)
    val (buildTypes2, buildJoinColsExp2, joinFilter2, probeTypes2, probeOutputCols2,
    probeHashColsExp2, buildOutputCols2, buildOutputTypes2, relation2) = genJoinOutput(join2)
    val (proj3OmniExpressions, proj3OmniInputTypes) = genProjectOutput(proj3)
    val (buildTypes3, buildJoinColsExp3, joinFilter3, probeTypes3, probeOutputCols3,
    probeHashColsExp3, buildOutputCols3, buildOutputTypes3, relation3) = genJoinOutput(join3)
    val (proj4OmniExpressions, proj4OmniInputTypes) = genProjectOutput(proj4)
    val (buildTypes4, buildJoinColsExp4, joinFilter4, probeTypes4, probeOutputCols4,
    probeHashColsExp4, buildOutputCols4, buildOutputTypes4, relation4) = genJoinOutput(join4)
    val (conditionExpression, omniCondInputTypes, omniCondExpressions) = genFilterOutput(filter)

    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      // for join
      val deserializer = VecBatchSerializerFactory.create()
      val startCodegen = System.nanoTime()
      val aggOperator = OmniAdaptorUtil.getAggOperator(aggregate.groupingExpressions,
        omniGroupByChanel,
        omniAggChannels,
        omniAggSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniAggInputRaw,
        omniAggOutputPartial)
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        aggOperator.close()
      })

      val projectOperatorFactory1 = new OmniProjectOperatorFactory(proj1OmniExpressions, proj1OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator1 = projectOperatorFactory1.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator1.close()
      })

      val buildOpFactory1 = new OmniHashBuilderWithExprOperatorFactory(buildTypes1,
        buildJoinColsExp1, if (joinFilter1.nonEmpty) {Optional.of(joinFilter1.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp1 = buildOpFactory1.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp1.close()
        buildOpFactory1.close()
      })

      relation1.value.buildData.foreach { input =>
        buildOp1.addInput(deserializer.deserialize(input))
      }
      buildOp1.getOutput
      val lookupOpFactory1 = new OmniLookupJoinWithExprOperatorFactory(probeTypes1, probeOutputCols1,
        probeHashColsExp1, buildOutputCols1, buildOutputTypes1, OMNI_JOIN_TYPE_INNER, buildOpFactory1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp1 = lookupOpFactory1.createOperator()
      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp1.close()
        lookupOpFactory1.close()
      })

      val projectOperatorFactory2 = new OmniProjectOperatorFactory(proj2OmniExpressions, proj2OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator2 = projectOperatorFactory2.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator2.close()
      })

      val buildOpFactory2 = new OmniHashBuilderWithExprOperatorFactory(buildTypes2,
        buildJoinColsExp2, if (joinFilter2.nonEmpty) {Optional.of(joinFilter2.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp2 = buildOpFactory2.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp2.close()
        buildOpFactory2.close()
      })

      relation2.value.buildData.foreach { input =>
        buildOp2.addInput(deserializer.deserialize(input))
      }
      buildOp2.getOutput
      val lookupOpFactory2 = new OmniLookupJoinWithExprOperatorFactory(probeTypes2, probeOutputCols2,
        probeHashColsExp2, buildOutputCols2, buildOutputTypes2, OMNI_JOIN_TYPE_INNER, buildOpFactory2,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp2 = lookupOpFactory2.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp2.close()
        lookupOpFactory2.close()
      })

      val projectOperatorFactory3 = new OmniProjectOperatorFactory(proj3OmniExpressions, proj3OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator3 = projectOperatorFactory3.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator3.close()
      })

      val buildOpFactory3 = new OmniHashBuilderWithExprOperatorFactory(buildTypes3,
        buildJoinColsExp3, if (joinFilter3.nonEmpty) {Optional.of(joinFilter3.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp3 = buildOpFactory3.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp3.close()
        buildOpFactory3.close()
      })

      relation3.value.buildData.foreach { input =>
        buildOp3.addInput(deserializer.deserialize(input))
      }
      buildOp3.getOutput
      val lookupOpFactory3 = new OmniLookupJoinWithExprOperatorFactory(probeTypes3, probeOutputCols3,
        probeHashColsExp3, buildOutputCols3, buildOutputTypes3, OMNI_JOIN_TYPE_INNER, buildOpFactory3,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp3 = lookupOpFactory3.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp3.close()
        lookupOpFactory3.close()
      })

      val projectOperatorFactory4 = new OmniProjectOperatorFactory(proj4OmniExpressions, proj4OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator4 = projectOperatorFactory4.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator4.close()
      })

      val buildOpFactory4 = new OmniHashBuilderWithExprOperatorFactory(buildTypes4,
        buildJoinColsExp4, if (joinFilter4.nonEmpty) {Optional.of(joinFilter4.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp4 = buildOpFactory4.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp4.close()
        buildOpFactory4.close()
      })

      relation4.value.buildData.foreach { input =>
        buildOp4.addInput(deserializer.deserialize(input))
      }
      buildOp4.getOutput
      val lookupOpFactory4 = new OmniLookupJoinWithExprOperatorFactory(probeTypes4, probeOutputCols4,
        probeHashColsExp4, buildOutputCols4, buildOutputTypes4, OMNI_JOIN_TYPE_INNER, buildOpFactory4,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp4 = lookupOpFactory4.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp4.close()
        lookupOpFactory4.close()
      })

      val condOperatorFactory = new OmniFilterAndProjectOperatorFactory(
        conditionExpression, omniCondInputTypes, seqAsJavaList(omniCondExpressions), 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val condOperator = condOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        condOperator.close()
      })

      while (batches.hasNext) {
        val batch = batches.next()
        val startInput = System.nanoTime()
        val input = transColBatchToOmniVecs(batch)
        val vecBatch = new VecBatch(input, batch.numRows())
        condOperator.addInput(vecBatch)
        val condOutput = condOperator.getOutput
        while (condOutput.hasNext) {
          val output = condOutput.next()
          lookupOp4.addInput(output)
          val joinOutput4 = lookupOp4.getOutput
          while (joinOutput4.hasNext) {
            val output = joinOutput4.next()
            projectOperator4.addInput(output)
            val projOutput4 = projectOperator4.getOutput
            while (projOutput4.hasNext) {
              val output = projOutput4.next()
              lookupOp3.addInput(output)
              val joinOutput3 = lookupOp3.getOutput
              while (joinOutput3.hasNext) {
                val output = joinOutput3.next()
                projectOperator3.addInput(output)
                val projOutput3 = projectOperator3.getOutput
                while (projOutput3.hasNext) {
                  val output = projOutput3.next()
                  lookupOp2.addInput(output)
                  val joinOutput2 = lookupOp2.getOutput
                  while (joinOutput2.hasNext) {
                    val output = joinOutput2.next()
                    projectOperator2.addInput(output)
                    val projOutput2 = projectOperator2.getOutput
                    while (projOutput2.hasNext) {
                      val output = projOutput2.next()
                      lookupOp1.addInput(output)
                      val joinOutput1 = lookupOp1.getOutput
                      while (joinOutput1.hasNext) {
                        val output = joinOutput1.next()
                        projectOperator1.addInput(output)
                        val proj1Output = projectOperator1.getOutput
                        while (proj1Output.hasNext) {
                          val output = proj1Output.next()
                          numInputRows += output.getRowCount()
                          aggOperator.addInput(output)
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
      }
      val startGetOp = System.nanoTime()
      val aggOutput = aggOperator.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
      val localSchema = aggregate.schema

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = aggOutput.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }
        override def next(): ColumnarBatch = {
          val vecBatch = aggOutput.next()
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, localSchema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(resultIdxToOmniResultIdxMap(i)))
          }
          numOutputRows += vecBatch.getRowCount
          numOutputVecBatchs += 1
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
    }
  }

  override val nodeNamePrefix: String = ""

  override val nodeName: String = "OmniColumnarMultipleOperatorExec"

  override protected def doCanonicalize(): SparkPlan = WrapperLeafExec()
}

case class ColumnarMultipleOperatorExec1(
    aggregate: HashAggregateExec,
    proj1: ColumnarProjectExec,
    join1: ColumnarBroadcastHashJoinExec,
    proj2: ColumnarProjectExec,
    join2: ColumnarBroadcastHashJoinExec,
    proj3: ColumnarProjectExec,
    join3: ColumnarBroadcastHashJoinExec,
    filter: ColumnarFilterExec,
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends BaseColumnarFileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  protected override def doPrepare(): Unit = {
    super.doPrepare()
    join1.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
    join2.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
    join3.getBuildPlan.asInstanceOf[ColumnarBroadcastExchangeExec].relationFuture
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "outputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "omniJitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    //operator metric
    "lookupAddInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni lookup addInput"),
    //
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ {
    if (relation.partitionSchemaOption.isDefined) {
      Map(
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  } ++ staticMetrics

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    val numInputRows = longMetric("numInputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniJitTime")
    val getOutputTime = longMetric("outputTime")

    val (omniGroupByChanel, omniAggChannels, omniAggSourceTypes, omniAggFunctionTypes, omniAggOutputTypes,
    omniAggInputRaw, omniAggOutputPartial, resultIdxToOmniResultIdxMap) = genAggOutput(aggregate)
    val (proj1OmniExpressions, proj1OmniInputTypes) = genProjectOutput(proj1)
    val (buildTypes1, buildJoinColsExp1, joinFilter1, probeTypes1, probeOutputCols1,
    probeHashColsExp1, buildOutputCols1, buildOutputTypes1, relation1, reserved1) = genJoinOutputWithReverse(join1)
    val (proj2OmniExpressions, proj2OmniInputTypes) = genProjectOutput(proj2)
    val (buildTypes2, buildJoinColsExp2, joinFilter2, probeTypes2, probeOutputCols2,
    probeHashColsExp2, buildOutputCols2, buildOutputTypes2, relation2, reserved2) = genJoinOutputWithReverse(join2)
    val (proj3OmniExpressions, proj3OmniInputTypes) = genProjectOutput(proj3)
    val (buildTypes3, buildJoinColsExp3, joinFilter3, probeTypes3, probeOutputCols3,
    probeHashColsExp3, buildOutputCols3, buildOutputTypes3, relation3, reserved3) = genJoinOutputWithReverse(join3)
    val (conditionExpression, omniCondInputTypes, omniCondExpressions) = genFilterOutput(filter)

    def reserveVec(o: VecBatch): VecBatch = {
      val omniVecs = o.getVectors
      val newOmniVecs = new Array[Vec](omniVecs.length)
      var index = 0
      for (i <- reserved3._1 until reserved3._2) {
        newOmniVecs(index) = omniVecs(i)
        index += 1
      }
      for (i <- reserved3._3 until reserved3._4) {
        newOmniVecs(index) = omniVecs(i)
        index += 1
      }
      o.close()
      new VecBatch(newOmniVecs)
    }

    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      // for join
      val deserializer = VecBatchSerializerFactory.create()
      val startCodegen = System.nanoTime()
      val aggOperator = OmniAdaptorUtil.getAggOperator(aggregate.groupingExpressions,
        omniGroupByChanel,
        omniAggChannels,
        omniAggSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniAggInputRaw,
        omniAggOutputPartial)
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        aggOperator.close()
      })

      val projectOperatorFactory1 = new OmniProjectOperatorFactory(proj1OmniExpressions, proj1OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator1 = projectOperatorFactory1.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator1.close()
      })

      val buildOpFactory1 = new OmniHashBuilderWithExprOperatorFactory(buildTypes1,
        buildJoinColsExp1, if (joinFilter1.nonEmpty) {Optional.of(joinFilter1.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp1 = buildOpFactory1.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp1.close()
        buildOpFactory1.close()
      })

      relation1.value.buildData.foreach { input =>
        buildOp1.addInput(deserializer.deserialize(input))
      }
      buildOp1.getOutput
      val lookupOpFactory1 = new OmniLookupJoinWithExprOperatorFactory(probeTypes1, probeOutputCols1,
        probeHashColsExp1, buildOutputCols1, buildOutputTypes1, OMNI_JOIN_TYPE_INNER, buildOpFactory1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp1 = lookupOpFactory1.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp1.close()
        lookupOpFactory1.close()
      })

      val projectOperatorFactory2 = new OmniProjectOperatorFactory(proj2OmniExpressions, proj2OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator2 = projectOperatorFactory2.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator2.close()
      })

      val buildOpFactory2 = new OmniHashBuilderWithExprOperatorFactory(buildTypes2,
        buildJoinColsExp2, if (joinFilter2.nonEmpty) {Optional.of(joinFilter2.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp2 = buildOpFactory2.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp2.close()
        buildOpFactory2.close()
      })

      relation2.value.buildData.foreach { input =>
        buildOp2.addInput(deserializer.deserialize(input))
      }
      buildOp2.getOutput
      val lookupOpFactory2 = new OmniLookupJoinWithExprOperatorFactory(probeTypes2, probeOutputCols2,
        probeHashColsExp2, buildOutputCols2, buildOutputTypes2, OMNI_JOIN_TYPE_INNER, buildOpFactory2,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp2 = lookupOpFactory2.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp2.close()
        lookupOpFactory2.close()
      })

      val projectOperatorFactory3 = new OmniProjectOperatorFactory(proj3OmniExpressions, proj3OmniInputTypes, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val projectOperator3 = projectOperatorFactory3.createOperator
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperator3.close()
      })

      val buildOpFactory3 = new OmniHashBuilderWithExprOperatorFactory(buildTypes3,
        buildJoinColsExp3, if (joinFilter3.nonEmpty) {Optional.of(joinFilter3.get)} else {Optional.empty()}, 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val buildOp3 = buildOpFactory3.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        buildOp3.close()
        buildOpFactory3.close()
      })

      relation3.value.buildData.foreach { input =>
        buildOp3.addInput(deserializer.deserialize(input))
      }
      buildOp3.getOutput
      val lookupOpFactory3 = new OmniLookupJoinWithExprOperatorFactory(probeTypes3, probeOutputCols3,
        probeHashColsExp3, buildOutputCols3, buildOutputTypes3, OMNI_JOIN_TYPE_INNER, buildOpFactory3,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val lookupOp3 = lookupOpFactory3.createOperator()

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        lookupOp3.close()
        lookupOpFactory3.close()
      })

      val condOperatorFactory = new OmniFilterAndProjectOperatorFactory(
        conditionExpression, omniCondInputTypes, seqAsJavaList(omniCondExpressions), 1,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val condOperator = condOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        condOperator.close()
      })

      while (batches.hasNext) {
        val batch = batches.next()
        val startInput = System.nanoTime()
        val input = transColBatchToOmniVecs(batch)
        val vecBatch = new VecBatch(input, batch.numRows())
        condOperator.addInput(vecBatch)
        val condOutput = condOperator.getOutput
        while (condOutput.hasNext) {
          val output = condOutput.next()
          lookupOp3.addInput(output)
          val joinOutput3 = lookupOp3.getOutput
          while (joinOutput3.hasNext) {
            val output = if (reserved3._1 > 0) {
              reserveVec(joinOutput3.next())
            } else {
              joinOutput3.next()
            }
            projectOperator3.addInput(output)
            val projOutput3 = projectOperator3.getOutput
            while (projOutput3.hasNext) {
              val output = projOutput3.next()
              lookupOp2.addInput(output)
              val joinOutput2 = lookupOp2.getOutput
              while (joinOutput2.hasNext) {
                val output = if (reserved2._1 > 0) {
                  reserveVec(joinOutput2.next())
                } else {
                  joinOutput2.next()
                }
                projectOperator2.addInput(output)
                val projOutput2 = projectOperator2.getOutput
                while (projOutput2.hasNext) {
                  val output = projOutput2.next()
                  lookupOp1.addInput(output)
                  val joinOutput1 = lookupOp1.getOutput
                  while (joinOutput1.hasNext) {
                    val output = if (reserved1._1 > 0) {
                      reserveVec(joinOutput1.next())
                    } else {
                      joinOutput1.next()
                    }
                    projectOperator1.addInput(output)
                    val proj1Output = projectOperator1.getOutput
                    while (proj1Output.hasNext) {
                      val output = proj1Output.next()
                      numInputRows += output.getRowCount()
                      aggOperator.addInput(output)
                    }
                  }
                }
              }
            }
          }
        }
        addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
      }
      val startGetOp = System.nanoTime()
      val aggOutput = aggOperator.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
      val localSchema = aggregate.schema

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = aggOutput.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }
        override def next(): ColumnarBatch = {
          val vecBatch = aggOutput.next()
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, localSchema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(resultIdxToOmniResultIdxMap(i)))
          }
          numOutputRows += vecBatch.getRowCount
          numOutputVecBatchs += 1
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
    }
  }

  override val nodeNamePrefix: String = ""

  override val nodeName: String = "ColumnarMultipleOperatorExec1"

  override protected def doCanonicalize(): SparkPlan = WrapperLeafExec()
}
