package com.huawei.boostkit.hive.cache;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe.TYPE_LEN;
import static com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe.getEstimateLen;

import com.huawei.boostkit.hive.shuffle.VecSerdeBody;

import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class VecBufferCache {
    public VecBuffer[] cache;
    public PrimitiveObjectInspector.PrimitiveCategory[] categories;

    private int[] columnTypeLen;

    public VecBufferCache(int colNum, List<TypeInfo> typeInfos) {
        this.categories = typeInfos.stream().map(typeInfo -> ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()).toArray(PrimitiveObjectInspector.PrimitiveCategory[]::new);
        columnTypeLen = new int[this.categories.length];
        cache = new VecBuffer[colNum];
        for (int i = 0; i < this.categories.length; i++) {
            columnTypeLen[i] = TYPE_LEN.getOrDefault(this.categories[i], 0);
            if (columnTypeLen[i] == 0) {
                cache[i] = new VecBuffer(getEstimateLen((PrimitiveTypeInfo) typeInfos.get(i)), true);
            } else {
                cache[i] = new VecBuffer(columnTypeLen[i], false);
            }
        }
    }

    public void addVecSerdeBody(VecSerdeBody[] vecSerdeBodies, int rowCount, int offset) {
        for (int i = 0; i < vecSerdeBodies.length; i++) {
            int cacheIndex = i + offset;
            System.arraycopy(vecSerdeBodies[i].value, 0, cache[cacheIndex].byteBuffer, cache[cacheIndex].offset[rowCount], vecSerdeBodies[i].length);
            if (cache[cacheIndex].isChar) {
                cache[cacheIndex].offset[rowCount + 1] = cache[cacheIndex].offset[rowCount] + vecSerdeBodies[i].length;
            }
            if (cache[cacheIndex].noNulls && vecSerdeBodies[i].isNull == 1) {
                cache[cacheIndex].noNulls = false;
            }
            cache[cacheIndex].isNull[rowCount] = vecSerdeBodies[i].isNull;
        }
    }

    public Vec[] getValueVecBatchCache(int rowCount) {
        if (rowCount == 0) {
            return null;
        }
        int vectorCount = cache.length;
        Vec[] vecs = new Vec[vectorCount];
        for (int i = 0; i < vectorCount; i++) {
            vecs[i] = buildVec(i, rowCount);
        }
        reset();
        return vecs;
    }

    public void reset() {
        for (int i = 0; i < this.categories.length; i++) {
            cache[i].noNulls = true;
        }
    }

    private Vec buildVec(int index, int rowCount) {
        Vec vec;
        switch (categories[index]) {
            case INT:
            case DATE:
                vec = new IntVec(rowCount);
                break;
            case LONG:
            case TIMESTAMP:
                vec = new LongVec(rowCount);
                break;
            case SHORT:
                vec = new ShortVec(rowCount);
                break;
            case BOOLEAN:
                vec = new BooleanVec(rowCount);
                break;
            case DOUBLE:
                vec = new DoubleVec(rowCount);
                break;
            case VARCHAR:
            case CHAR:
            case STRING:
                vec = new VarcharVec(cache[index].offset[rowCount], rowCount);
                if (rowCount == BATCH) {
                    ((VarcharVec) vec).setOffsetBuf(cache[index].offset, rowCount);
                } else {
                    int[] offsets = new int[rowCount + 1];
                    System.arraycopy(cache[index].offset, 0, offsets, 0, rowCount + 1);
                    ((VarcharVec) vec).setOffsetBuf(offsets, rowCount);
                }
                break;
            case DECIMAL:
                vec = new Decimal128Vec(rowCount);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + categories[index]);
        }
        vec.setValuesBuf(cache[index].byteBuffer, cache[index].offset[rowCount]);
        if (!cache[index].noNulls) {
            vec.setNullsBuf(cache[index].isNull, rowCount);
        }
        return vec;
    }
}
