package scr.main.scala.com.huawei.booskit.spark.util

object OmniAdaptorUtil {
  def transColBatchToOmniVecs(cb: ColumnarBatch, isSlice: Boolean): Array[Vec] = {
    transColBatchToOmniVecs(cb, false)
  }

  def transColBatchToOmniVecs(cb: ColumnarBatch, isSlice: Boolean): Array[Vec] = {
    val input = new Array[Vec](cb.numCols())
    for (i <- 0 until cb.numCols()) {
      val omniVec: Vec = cb.column(i) match {
        case vector: OrcColumnVector =>
          transColumnVector(vector, cb.numCols())
        case vector: OnHeapColumnVector =>
          transColumnVector(vector, cb.numCols())
        case vector: OmniColumnVector =>
          if (!isSlice) {
            vector.getVec
          } else {
            vector.getVec.slice(0, cb.numCols())
          }
        case _ =>
          throw new RuntimeException("unsupport column vector!")
      }
      input(i) = omniVec
    }
    input
  }

  def transColumnVector(columnVector: OrcColumnVector, columnSize: Int): Vec = {
    val dataType: DataType = columnVector.dataType()
    val vec: Vec = dataType match {
      case LongType =>
        val vec = new LongVec(columnSize)
        val values = new Array[Long](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getLong(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case DataType | IntegerType =>
        val vec = new IntVec(columnSize)
        val values = new Array[Int](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getInt(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case ShortType =>
        val vec = new ShortVec(columnSize)
        val values = new Array[Short](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getShort(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case DoubleType =>
        val vec = new DoubleVec(columnSize)
        val values = new Array[Double](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getDouble(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case StringType =>
        var totalSize = 0
        val offsets = new Array[Int](columnSize + 1)
        for (i <- 0 until columnSize) {
          if (null != columnVector.getUTF8String(i)) {
            val strLen: Int = columnVector.getUTF8String(i).getBytes.length
            totalSize += strLen
          }
          offsets(i + 1) = totalSize
        }
        val vec = new VarcharVec(totalSize, columnSize)
        val values = new Array[Byte](totalSize)
        for (i <- 0 until columnSize) {
          if (null != columnVector.getUTF8String(i)) {
            System.arraycopy(columnVector.getUTF8String(i).getBytes, 0, values,
              offsets(i), offsets(i + 1) - offsets(i))
          } else {
            vec.setNull(i)
          }
        }
        vec.put(0, values, 0, offsets, 0, columnSize)
        vec
      case BooleanType =>
        val vec = new BooleanVec(columnSize)
        val values = new Array[Boolean](columnSize)
        for (i <- 0 until columnSize) {
          if (!columnVector.isNullAt(i)) {
            values(i) = columnVector.getBoolean(i)
          } else {
            vec.setNull(i)
          }
        }
        vec.put(values, 0, 0, columnSize)
        vec
      case DecimalType =>
        if (DecimalType.is64BitDecimalType(dataType)) {
          val vec = new LongVec(columnSize)
          val values = new Array[Long](columnSize)
          for (i <- 0 until columnSize) {
            if (!columnVector.isNullAt(i)) {
              values(i) = columnVector.getDecimal(i, t.precision, t.scale).toUnscaledLong
            } else {
              vec.setNull(i)
            }
          }
          vec.put(values, 0, 0, columnSize)
          vec
        } else {
          val vec = new Decimal128Vec(columnSize)
          for (i <- 0 until columnSize) {
            if (!columnVector.isNullAt(i)) {
              vec.setBigInteger(i,
                columnVector.getDecimal(i, t.precision, t.scale).toJavaBigDecimal.unscaledValue())
            } else {
              vec.setNull(i)
            }
          }
          vec
        }
      case _ =>
        throw new UnsupportedOperationException("unsupport column vector!")
    }
    vec
  }

  def genSortParam(output: Seq[Attribute], sortOrder: Seq[SortOrder]):
    (Array[nova.hetu.runtime.`type`.DataType], Array[Int], Array[Int], Array[String]) = {
    val inputColSize: Int = output.size
    val sourceTypes = new Array[nova.hetu.runtime.`type`.DataType](inputColSize)
    val ascendings = new Array[Int](sortOrder.size)
    val nullFirsts = new Array[Int](sortOrder.size)
    val sortColsExp = new Array[String](sortOrder.size)
    val omniAttrExpsIdMap: Map[ExprId, Int] = getExprIdMap(output)

    output.zipWithIndex.foreach { case (inputAttr, i) =>
      sourceTypes(i) = sparkTypeToOmni(inputAttr.dataType, inputAttr.metadata)
    }
    sortOrder.zipWithIndex.foreach {case (sortAttr, i) =>
      sortColsExp(i) = rewriteToOmniJsonExpressionLiteral(sortAttr.child, omniAttrExpsIdMap)
      ascendings(i) = if (sortAttr.isAscending) {
        1
      } else {
        0
      }
      nullFirsts(i) = sortAttr.nullOrdering.sql match {
        case "NULLS LAST" => 0
        case _ => 1
      }
    }
    (sourceTypes, ascendings, nullFirsts, sortColsExp)
  }

  def addAllAndGetIterator(operator: OmniOperator,
                           inputIter: Iterator[ColumnarBatch], schema: StructType,
                           addInputTime: SQLMetric, numInputVecBatchs: SQLMetric,
                           numInputRows: SQLMetric, getOutputTime: SQLMetric,
                           numOutputVecBatchs: SQLMetric, numOutputRows: SQLMetric,
                           outputDataSize: SQLMetric): Iterator[ColumnarBatch] = {
    while (inputIter.hasNext) {
      val batch: ColumnarBatch = inputIter.next()
      numInputVecBatchs += 1
      val input: Array[Vec] = transColBatchToOmniVecs(batch)
      val vecBatch = new VecBatch(input, batch.numRows())
      val startInput: Long = System.nanoTime()
      operator.addInput(vecBatch)
      addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
      numInputRows += batch.numRows()
    }
    val startGetOp: Long = System.nanoTime()
    val results: util.Iterator[VecBatch] = operator.getOutput
    getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        val startGetOp: Long = System.nanoTime()
        var hasNext = results.hasNext
        getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
        hasNext
      }

      override def next(): ColumnarBatch = {
        val startGetOp: Long = System.nanoTime()
        val vecBatch: VecBatch = results.next()
        getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
        val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
          vecBatch.getRowCount, schema, false)
        vectors.zipWithIndex.foreach {
          case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(i))
            outputDataSize += vecBatch.getVectors()(i).getRealValueBufCapacityInBytes
            outputDataSize += vecBatch.getVectors()(i).getRealNullBufCapacityInBytes
            outputDataSize += vecBatch.getVectors()(i).getRealOffsetBufCapacityInBytes
        }
        // metrics
        val rowCnt: Int = vecBatch.getRowCount
        numOutputRows += rowCnt
        numOutputVecBatchs += 1
        // close omni vecbatch
        vecBatch.close()
        new ColumnarBatch(vectors.toArray, rowCnt)
      }
    }
  }
}
