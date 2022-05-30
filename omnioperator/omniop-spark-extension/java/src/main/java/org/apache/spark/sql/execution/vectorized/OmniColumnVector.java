/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution.vectorized;

import nova.hetu.omniruntime.vector.*;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * OmniColumnVector
 */
public class OmniColumnVector extends WritableColumnVector {
    private static final boolean BIG_ENDIAN_PLATFORM = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

    /**
     * Allocates columns to store elements of each field of the schema on heap.
     * Capacity is the initial capacity of the vector and it will grow as necessary.
     * Capacity is in number of elements, not number of bytes.
     */
    public static OmniColumnVector[] allocateColumns(int capacity, StructType schema, boolean initVec) {
        return allocateColumns(capacity, schema.fields(), initVec);
    }

    /**
     * Allocates columns to store elements of each field on heap. Capacity is the
     * initial capacity of the vector and it will grow as necessary. Capacity is in
     * number of elements, not number of bytes.
     */
    public static OmniColumnVector[] allocateColumns(int capacity, StructField[] fields, boolean initVec) {
        OmniColumnVector[] vectors = new OmniColumnVector[fields.length];
        for (int i = 0; i < fields.length; i++) {
            vectors[i] = new OmniColumnVector(capacity, fields[i].dataType(), initVec);
        }
        return vectors;
    }

    // The data stored in these arrays need to maintain binary compatible. We can
    // directly pass this buffer to external components.
    // This is faster than a boolean array and we optimize this over memory
    // footprint.
    // Array for each type. Only 1 is populated for any type.
    private BooleanVec booleanDataVec;
    private ShortVec shortDataVec;
    private IntVec intDataVec;
    private LongVec longDataVec;
    private DoubleVec doubleDataVec;
    private Decimal128Vec decimal128DataVec;
    private VarcharVec charsTypeDataVec;
    private DictionaryVec dictionaryData;

    // init vec
    private boolean initVec;

    public OmniColumnVector(int capacity, DataType type, boolean initVec) {
        super(capacity, type);
        this.initVec = initVec;
        if (this.initVec) {
            reserveInternal(capacity);
        }
        reset();
    }

    /**
     * get vec
     *
     * @return Vec
     */
    public Vec getVec() {
        if (dictionaryData != null) {
            return dictionaryData;
        }

        if (type instanceof LongType) {
            return longDataVec;
        } else if (type instanceof BooleanType) {
            return booleanDataVec;
        } else if (type instanceof ShortType) {
            return shortDataVec;
        } else if (type instanceof IntegerType) {
            return intDataVec;
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                return longDataVec;
            } else {
                return decimal128DataVec;
            }
        } else if (type instanceof DoubleType) {
            return doubleDataVec;
        } else if (type instanceof StringType) {
            return charsTypeDataVec;
        } else if (type instanceof DateType) {
            return intDataVec;
        } else if (type instanceof ByteType) {
            return charsTypeDataVec;
        } else {
            return null;
        }
    }

    /**
     * set Vec
     *
     * @param vec Vec
     */
    public void setVec(Vec vec) {
        if (vec instanceof DictionaryVec) {
            dictionaryData = (DictionaryVec) vec;
        } else if (type instanceof LongType) {
            this.longDataVec = (LongVec) vec;
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                this.longDataVec = (LongVec) vec;
            } else {
                this.decimal128DataVec = (Decimal128Vec) vec;
            }
        } else if (type instanceof BooleanType) {
            this.booleanDataVec = (BooleanVec) vec;
        } else if (type instanceof ShortType) {
            this.shortDataVec = (ShortVec) vec;
        } else if (type instanceof IntegerType) {
            this.intDataVec = (IntVec) vec;
        } else if (type instanceof DoubleType) {
            this.doubleDataVec = (DoubleVec) vec;
        } else if (type instanceof StringType) {
            this.charsTypeDataVec = (VarcharVec) vec;
        } else if (type instanceof DateType) {
            this.intDataVec = (IntVec) vec;
        } else if (type instanceof ByteType) {
            this.charsTypeDataVec = (VarcharVec) vec;
        } else {
            return;
        }
    }

    @Override
    public void close() {
        super.close();
        if (booleanDataVec != null) {
            booleanDataVec.close();
        }
        if (shortDataVec != null) {
            shortDataVec.close();
        }
        if (intDataVec != null) {
            intDataVec.close();
        }
        if (longDataVec != null) {
            longDataVec.close();
        }
        if (doubleDataVec != null) {
            doubleDataVec.close();
        }
        if (decimal128DataVec != null) {
            decimal128DataVec.close();
        }
        if (charsTypeDataVec != null) {
            charsTypeDataVec.close();
        }
        if (dictionaryData != null) {
            dictionaryData.close();
            dictionaryData = null;
        }
    }

    //
    // APIs dealing with nulls
    //

    @Override
    public boolean hasNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numNulls() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNotNull(int rowId) {}

    @Override
    public void putNull(int rowId) {
        if (dictionaryData != null) {
            dictionaryData.setNull(rowId);
            return;
        }
        if (type instanceof BooleanType) {
            booleanDataVec.setNull(rowId);
        } else if (type instanceof ByteType) {
            charsTypeDataVec.setNull(rowId);
        } else if (type instanceof ShortType) {
            shortDataVec.setNull(rowId);
        } else if (type instanceof IntegerType) {
            intDataVec.setNull(rowId);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec.setNull(rowId);
            } else {
                decimal128DataVec.setNull(rowId);
            }
        } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type)) {
            longDataVec.setNull(rowId);
        } else if (type instanceof FloatType) {
            return;
        } else if (type instanceof DoubleType) {
            doubleDataVec.setNull(rowId);
        } else if (type instanceof StringType) {
            charsTypeDataVec.setNull(rowId);
        } else if (type instanceof DateType) {
            intDataVec.setNull(rowId);
        }
    }

    @Override
    public void putNulls(int rowId, int count) {
        boolean[] nullValue = new boolean[count];
        Arrays.fill(nullValue, true);
        if (dictionaryData != null) {
            dictionaryData.setNulls(rowId, nullValue, 0, count);
            return;
        }
        if (type instanceof BooleanType) {
            booleanDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ByteType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof ShortType) {
            shortDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof IntegerType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec.setNulls(rowId, nullValue, 0, count);
            } else {
                decimal128DataVec.setNulls(rowId, nullValue, 0, count);
            }
        } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type)) {
            longDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof FloatType) {
            return;
        } else if (type instanceof DoubleType) {
            doubleDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof StringType) {
            charsTypeDataVec.setNulls(rowId, nullValue, 0, count);
        } else if (type instanceof DateType) {
            intDataVec.setNulls(rowId, nullValue, 0, count);
        }
    }

    @Override
    public void putNotNulls(int rowId, int count) {}

    @Override
    public boolean isNullAt(int rowId) {
        if (dictionaryData != null) {
            return dictionaryData.isNull(rowId);
        }
        if (type instanceof BooleanType) {
            return booleanDataVec.isNull(rowId);
        } else if (type instanceof ByteType) {
            return charsTypeDataVec.isNull(rowId);
        } else if (type instanceof ShortType) {
            return shortDataVec.isNull(rowId);
        } else if (type instanceof IntegerType) {
            return intDataVec.isNull(rowId);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                return longDataVec.isNull(rowId);
            } else {
                return decimal128DataVec.isNull(rowId);
            }
        } else if (type instanceof LongType || DecimalType.is64BitDecimalType(type)) {
            return longDataVec.isNull(rowId);
        } else if (type instanceof FloatType) {
            return false;
        } else if (type instanceof DoubleType) {
            return doubleDataVec.isNull(rowId);
        } else if (type instanceof StringType) {
            return charsTypeDataVec.isNull(rowId);
        } else if (type instanceof DateType) {
            return intDataVec.isNull(rowId);
        } else {
            throw new RuntimeException("unknown type for " + rowId);
        }
    }

    //
    // APIs dealing with Booleans
    //

    @Override
    public void putBoolean(int rowId, boolean value) {
        booleanDataVec.set(rowId, value);
    }

    @Override
    public void putBooleans(int rowId, int count, boolean value) {
        for (int i = 0; i < count; ++i) {
            booleanDataVec.set(i + rowId, value);
        }
    }

    @Override
    public boolean getBoolean(int rowId) {
        if (dictionaryData != null) {
            return dictionaryData.getBoolean(rowId);
        }
        return booleanDataVec.get(rowId);
    }

    @Override
    public boolean[] getBooleans(int rowId, int count) {
        assert (dictionary == null);
        boolean[] array = new boolean[count];
        for (int i = 0; i < count; ++i) {
            array[i] = booleanDataVec.get(rowId + i);
        }
        return array;
    }

    //

    //
    // APIs dealing with Bytes
    //

    @Override
    public void putByte(int rowId, byte value) {
        charsTypeDataVec.set(rowId, new byte[]{value});
    }

    @Override
    public void putBytes(int rowId, int count, byte value) {
        for (int i = 0; i < count; ++i) {
            charsTypeDataVec.set(rowId, new byte[]{value});
        }
    }

    @Override
    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
        byte[] array = new byte[count];
        System.arraycopy(src, srcIndex, array, 0, count);
        charsTypeDataVec.set(rowId, array);
    }

    /**
     *
     * @param length length of string value
     * @param src src value
     * @param offset offset value
     * @return return count of elements
     */
    public final int appendString(int length, byte[] src, int offset) {
        reserve(elementsAppended + 1);
        int result = elementsAppended;
        putBytes(elementsAppended, length, src, offset);
        elementsAppended++;
        return result;
    }

    @Override
    public byte getByte(int rowId) {
        if (dictionary != null) {
            return (byte) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getBytes(rowId)[0];
        } else {
            return charsTypeDataVec.get(rowId)[0];
        }
    }

    @Override
    public byte[] getBytes(int rowId, int count) {
        assert (dictionary == null);
        byte[] array = new byte[count];
        for (int i = 0; i < count; i++) {
            if (type instanceof StringType) {
                array[i] = ((VarcharVec) ((OmniColumnVector) getChild(0)).getVec()).get(rowId + i)[0];
            } else if (type instanceof ByteType) {
                array[i] = charsTypeDataVec.get(rowId + i)[0];
            } else {
                throw new RuntimeException("Unsupported putShorts");
            }
        }
        return array;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (dictionaryData != null) {
            return UTF8String.fromBytes(dictionaryData.getBytes(rowId));
        } else {
            return UTF8String.fromBytes(charsTypeDataVec.get(rowId));
        }
    }

    @Override
    protected UTF8String getBytesAsUTF8String(int rowId, int count) {
        return UTF8String.fromBytes(getBytes(rowId, count), rowId, count);
    }

    //
    // APIs dealing with Shorts
    //

    @Override
    public void putShort(int rowId, short value) {
        shortDataVec.set(rowId, value);
    }

    @Override
    public void putShorts(int rowId, int count, short value) {
        for (int i = 0; i < count; ++i) {
            shortDataVec.set(i + rowId, value);
        }
    }

    @Override
    public void putShorts(int rowId, int count, short[] src, int srcIndex) {
        shortDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
        throw new RuntimeException("Unsupported putShorts");
    }

    @Override
    public short getShort(int rowId) {
        if (dictionary != null) {
            return (short) dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            throw new UnsupportedOperationException("Unsupported to get short from dictionary vector");
        } else {
            return shortDataVec.get(rowId);
        }
    }

    @Override
    public short[] getShorts(int rowId, int count) {
        assert (dictionary == null);
        short[] array = new short[count];
        for (int i = 0; i < count; i++) {
            array[i] = shortDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with Ints
    //

    @Override
    public void putInt(int rowId, int value) {
        intDataVec.set(rowId, value);
    }

    @Override
    public void putInts(int rowId, int count, int value) {
        for (int i = 0; i < count; ++i) {
            intDataVec.set(rowId + i, value);
        }
    }

    @Override
    public void putInts(int rowId, int count, int[] src, int srcIndex) {
        intDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putInts(int rowId, int count, byte[] src, int srcIndex) {
        throw new RuntimeException("Unsupported putInts");
    }

    @Override
    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
        for (int i = 0; i < count; ++i, srcOffset += 4) {
            intDataVec.set(rowId + i, Platform.getInt(src, srcOffset));
            if (BIG_ENDIAN_PLATFORM) {
                intDataVec.set(rowId + i, Integer.reverseBytes(intDataVec.get(i + rowId)));
            }
        }
    }

    @Override
    public int getInt(int rowId) {
        if (dictionary != null) {
            return dictionary.decodeToInt(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getInt(rowId);
        } else {
            return intDataVec.get(rowId);
        }
    }

    @Override
    public int[] getInts(int rowId, int count) {
        assert (dictionary == null);
        int[] array = new int[count];
        for (int i = 0; i < count; i++) {
            array[i] = intDataVec.get(rowId + i);
        }
        return array;
    }

    /**
     * Returns the dictionary Id for rowId. This should only be called when the
     * ColumnVector is dictionaryIds. We have this separate method for dictionaryIds
     * as per SPARK-16928.
     */
    public int getDictId(int rowId) {
        assert (dictionary == null) : "A ColumnVector dictionary should not have a dictionary for itself.";
        return intDataVec.get(rowId);
    }

    //
    // APIs dealing with Longs
    //

    @Override
    public void putLong(int rowId, long value) {
        longDataVec.set(rowId, value);
    }

    @Override
    public void putLongs(int rowId, int count, long value) {
        for (int i = 0; i < count; ++i) {
            longDataVec.set(i + rowId, value);
        }
    }

    @Override
    public void putLongs(int rowId, int count, long[] src, int srcIndex) {
        longDataVec.put(src, rowId, srcIndex, count);
    }

    @Override
    public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
        throw new RuntimeException("Unsupported putLongs");
    }

    @Override
    public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
        for (int i = 0; i < count; ++i, srcOffset += 8) {
            longDataVec.set(i + rowId, Platform.getLong(src, srcOffset));
            if (BIG_ENDIAN_PLATFORM) {
                longDataVec.set(i + rowId, Long.reverseBytes(longDataVec.get(i + rowId)));
            }
        }
    }

    @Override
    public long getLong(int rowId) {
        if (dictionary != null) {
            return dictionary.decodeToLong(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getLong(rowId);
        } else {
            return longDataVec.get(rowId);
        }
    }

    @Override
    public long[] getLongs(int rowId, int count) {
        assert (dictionary == null);
        long[] array = new long[count];
        for (int i = 0; i < count; i++) {
            array[i] = longDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with floats, omni-vector not support float data type
    //

    @Override
    public void putFloat(int rowId, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putFloats(int rowId, int count, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putFloats(int rowId, int count, float[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloats(int rowId, int count) {
        throw new UnsupportedOperationException();
    }

    //
    // APIs dealing with doubles
    //

    @Override
    public void putDouble(int rowId, double value) {
        doubleDataVec.set(rowId, value);
    }

    @Override
    public void putDoubles(int rowId, int count, double value) {
        for (int i = 0; i < count; i++) {
            doubleDataVec.set(rowId + i, value);
        }
    }

    @Override
    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
        throw new RuntimeException("Unsupported putDoubles");
    }

    @Override
    public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
        throw new RuntimeException("Unsupported putDoubles");
    }

    @Override
    public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        if (!BIG_ENDIAN_PLATFORM) {
            putDoubles(rowId, count, src, srcIndex);
        } else {
            ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; ++i) {
                doubleDataVec.set(i + rowId, bb.getDouble(srcIndex + (8 * i)));
            }
        }
    }

    @Override
    public double getDouble(int rowId) {
        if (dictionary != null) {
            return dictionary.decodeToDouble(dictionaryIds.getDictId(rowId));
        } else if (dictionaryData != null) {
            return dictionaryData.getDouble(rowId);
        } else {
            return doubleDataVec.get(rowId);
        }
    }

    @Override
    public double[] getDoubles(int rowId, int count) {
        assert (dictionary == null);
        double[] array = new double[count];
        for (int i = 0; i < count; i++) {
            array[i] = doubleDataVec.get(rowId + i);
        }
        return array;
    }

    //
    // APIs dealing with Arrays
    //

    @Override
    public int getArrayLength(int rowId) {
        throw new RuntimeException("Unsupported getArrayLength");
    }

    @Override
    public int getArrayOffset(int rowId) {
        throw new RuntimeException("Unsupported getArrayOffset");
    }

    @Override
    public void putArray(int rowId, int offset, int length) {
        throw new RuntimeException("Unsupported putArray");
    }

    //
    // APIs dealing with Byte Arrays
    //

    @Override
    public int putByteArray(int rowId, byte[] value, int offset, int length) {
        throw new RuntimeException("Unsupported putByteArray");
    }

    /**
     *
     * @param value BigDecimal
     * @return return count of elements
     */
    public final int appendDecimal(Decimal value)
    {
        reserve(elementsAppended + 1);
        int result = elementsAppended;
        if (value.precision() <= Decimal.MAX_LONG_DIGITS()) {
            longDataVec.set(elementsAppended, value.toUnscaledLong());
        } else {
            decimal128DataVec.setBigInteger(elementsAppended, value.toJavaBigInteger());
        }
        elementsAppended++;
        return result;
    }

    @Override
    public void putDecimal(int rowId, Decimal value, int precision) {
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            longDataVec.set(rowId, value.toUnscaledLong());
        } else {
            decimal128DataVec.setBigInteger(rowId, value.toJavaBigInteger());
        }
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) return null;
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            return Decimal.apply(getLong(rowId), precision, scale);
        } else {
            return Decimal.apply(new BigDecimal(decimal128DataVec.getBigInteger(rowId), scale), precision, scale);
        }
    }

    @Override
    public boolean isArray() {
        return false;
    }

    // Spilt this function out since it is the slow path。
    @Override
    protected void reserveInternal(int newCapacity) {
        if (type instanceof BooleanType) {
            booleanDataVec = new BooleanVec(newCapacity);
        } else if (type instanceof ByteType) {
            charsTypeDataVec = new VarcharVec(newCapacity * 4, newCapacity);
        } else if (type instanceof ShortType) {
            shortDataVec = new ShortVec(newCapacity);
        } else if (type instanceof IntegerType) {
            intDataVec = new IntVec(newCapacity);
        } else if (type instanceof DecimalType) {
            if (DecimalType.is64BitDecimalType(type)) {
                longDataVec = new LongVec(newCapacity);
            } else {
                decimal128DataVec = new Decimal128Vec(newCapacity);
            }
        } else if (type instanceof LongType) {
            longDataVec = new LongVec(newCapacity);
        } else if (type instanceof FloatType) {
            throw new UnsupportedOperationException();
        } else if (type instanceof DoubleType) {
            doubleDataVec = new DoubleVec(newCapacity);
        } else if (type instanceof StringType) {
            // need to set with real column size, suppose char(200) utf8
            charsTypeDataVec = new VarcharVec(newCapacity * 4 * 200, newCapacity);
        } else if (type instanceof DateType) {
            intDataVec = new IntVec(newCapacity);
        } else {
            throw new RuntimeException("Unhandled " + type);
        }
        capacity = newCapacity;
    }

    @Override
    protected OmniColumnVector reserveNewColumn(int capacity, DataType type) {
        return new OmniColumnVector(capacity, type, true);
    }
}
