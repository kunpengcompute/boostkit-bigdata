package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * OmniColumnVector
 */
public class OmniColumnVector extends WritableColumnVector {
    public OmniColumnVector(int capacity, DataType type, boolean initVec) {
        super(capacity, type);
    }

    @Override
    public int getDictId(int rowId) {
        return 0;
    }

    @Override
    protected void reserveInternal(int capacity) {

    }

    @Override
    public void putNotNull(int rowId) {

    }

    @Override
    public void putNull(int rowId) {

    }

    @Override
    public void putNulls(int rowId, int count) {

    }

    @Override
    public void putNotNulls(int rowId, int count) {

    }

    @Override
    public void putBoolean(int rowId, boolean value) {

    }

    @Override
    public void putBooleans(int rowId, int count, boolean value) {

    }

    @Override
    public void putByte(int rowId, byte value) {

    }

    @Override
    public void putBytes(int rowId, int count, byte value) {

    }

    @Override
    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putShort(int rowId, short value) {

    }

    @Override
    public void putShorts(int rowId, int count, short value) {

    }

    @Override
    public void putShorts(int rowId, int count, short[] src, int srcIndex) {

    }

    @Override
    public void putShorts(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putInt(int rowId, int value) {

    }

    @Override
    public void putInts(int rowId, int count, int value) {

    }

    @Override
    public void putInts(int rowId, int count, int[] src, int srcIndex) {

    }

    @Override
    public void putInts(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putLong(int rowId, long value) {

    }

    @Override
    public void putLongs(int rowId, int count, long value) {

    }

    @Override
    public void putLongs(int rowId, int count, long[] src, int srcIndex) {

    }

    @Override
    public void putLongs(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putFloat(int rowId, float value) {

    }

    @Override
    public void putFloats(int rowId, int count, float value) {

    }

    @Override
    public void putFloats(int rowId, int count, float[] src, int srcIndex) {

    }

    @Override
    public void putFloats(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putDouble(int rowId, double value) {

    }

    @Override
    public void putDoubles(int rowId, int count, double value) {

    }

    @Override
    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {

    }

    @Override
    public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

    }

    @Override
    public void putArray(int rowId, int offset, int length) {

    }

    @Override
    public int putByteArray(int rowId, byte[] value, int offset, int count) {
        return 0;
    }

    @Override
    protected UTF8String getBytesAsUTF8String(int rowId, int count) {
        return null;
    }

    @Override
    public int getArrayLength(int rowId) {
        return 0;
    }

    @Override
    public int getArrayOffset(int rowId) {
        return 0;
    }

    @Override
    protected WritableColumnVector reserveNewColumn(int capacity, DataType type) {
        return null;
    }

    @Override
    public boolean isNullAt(int rowId) {
        return false;
    }

    @Override
    public boolean getBoolean(int rowId) {
        return false;
    }

    @Override
    public byte getByte(int rowId) {
        return 0;
    }

    @Override
    public short getShort(int rowId) {
        return 0;
    }

    @Override
    public int getInt(int rowId) {
        return 0;
    }

    @Override
    public long getLong(int rowId) {
        return 0;
    }

    @Override
    public float getFloat(int rowId) {
        return 0;
    }

    @Override
    public double getDouble(int rowId) {
        return 0;
    }
}
