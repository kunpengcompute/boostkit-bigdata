package com.huawei.boostkit.hbase.index;

public class OffheapLruCache<KEYTYPE> {
  public OffheapLruCache(long maxMemorySize, float reserveRatio, long evaluatedBlockSize) {
  }

  public void putCache(KEYTYPE cacheKey, OffheapReplaceableCache cacheable)
      throws InsufficientMemoryException, IllegalMemoryRequestException {
  }

  public OffheapReplaceableCache getCache(KEYTYPE cacheKey) {
      return null;
  }

  public boolean containsKey(KEYTYPE cacheKey) {
    return false;
  }

  public void returnCache(OffheapReplaceableCache value) {
  }

  public long getMaxMemory() {
    return 0; 
  }

  public void shutdown() {
  }
  public void activateEvictThread(){}
}
