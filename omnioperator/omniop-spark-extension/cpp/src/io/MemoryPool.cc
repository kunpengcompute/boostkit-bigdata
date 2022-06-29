/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "MemoryPool.hh"

#include "Adaptor.hh"

#include <cstdlib>
#include <iostream>
#include <string.h>

namespace spark {

  MemoryPool::~MemoryPool() {
    // PASS
  }

  class MemoryPoolImpl: public MemoryPool {
  public:
    virtual ~MemoryPoolImpl() override;

    char* malloc(uint64_t size) override;
    void free(char* p) override;
  };

  char* MemoryPoolImpl::malloc(uint64_t size) {
    return static_cast<char*>(std::malloc(size));
  }

  void MemoryPoolImpl::free(char* p) {
    std::free(p);
  }

  MemoryPoolImpl::~MemoryPoolImpl() {
    // PASS
  }

  template <class T>
  DataBuffer<T>::DataBuffer(MemoryPool& pool,
                            uint64_t newSize
                            ): memoryPool(pool),
                               buf(nullptr),
                               currentSize(0),
                               currentCapacity(0) {
    resize(newSize);
  }

  template <class T>
  DataBuffer<T>::~DataBuffer() {
    for (uint64_t i = currentSize; i > 0; --i) {
      (buf + i - 1)->~T();
    }
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <class T>
  void DataBuffer<T>::resize(uint64_t newSize) {
    reserve(newSize);
    if (currentSize > newSize) {
      for (uint64_t i = currentSize; i > newSize; --i) {
        (buf + i -1)->~T();
      }
    } else if (newSize > currentSize) {
      for (uint64_t i = currentSize; i < newSize; ++i) {
        new (buf + i) T();
      }
    }
    currentSize = newSize;
  }

  template <class T>
  void DataBuffer<T>::reserve(uint64_t newCapacity) {
    if (newCapacity > currentCapacity || !buf) {
      if (buf) {
        T* buf_old = buf;
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
        memcpy(buf, buf_old, sizeof(T) * currentSize);
        memoryPool.free(reinterpret_cast<char*>(buf_old));
      } else {
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
      }
      currentCapacity = newCapacity;
    }
  }

  // Specializations for char

  template <>
  DataBuffer<char>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
  }

  // Specializations for unsigned char

  template <>
  DataBuffer<unsigned char>::~DataBuffer() {
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <>
  void DataBuffer<unsigned char>::resize(uint64_t newSize) {
    reserve(newSize);
    if (newSize > currentSize) {
      memset(buf + currentSize, 0, newSize - currentSize);
    }
    currentSize = newSize;
  }

  #ifdef __clang__
    #pragma clang diagnostic ignored "-Wweak-template-vtables"
  #endif

  template class DataBuffer<char>;
  template class DataBuffer<char*>;
  template class DataBuffer<double>;
  template class DataBuffer<int64_t>;
  template class DataBuffer<uint64_t>;
  template class DataBuffer<unsigned char>;

  #ifdef __clang__
    #pragma clang diagnostic ignored "-Wexit-time-destructors"
  #endif

  MemoryPool* getDefaultPool() {
    static MemoryPoolImpl internal;
    return &internal;
  }
} // namespace spark