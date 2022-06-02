/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#ifndef MEMORYPOOL_HH_
#define MEMORYPOOL_HH_

#include <memory>

namespace spark {

  class MemoryPool {
  public:
    virtual ~MemoryPool();

    virtual char* malloc(uint64_t size) = 0;
    virtual void free(char* p) = 0;
  };
  MemoryPool* getDefaultPool();

  template <class T>
  class DataBuffer {
  private:
    MemoryPool& memoryPool;
    T* buf;
    // current size
    uint64_t currentSize;
    // maximal capacity (actual allocated memory)
    uint64_t currentCapacity;

    // not implemented
    DataBuffer(DataBuffer& buffer);
    DataBuffer& operator = (DataBuffer& buffer);

  public:
    DataBuffer(MemoryPool& pool, uint64_t _size = 0);
    virtual ~DataBuffer();

    T* data() {
      return buf;
    }

    const T* data() const {
      return buf;
    }

    uint64_t size() {
      return currentSize;
    }

    uint64_t capacity() {
      return currentCapacity;
    }

    T& operator[](uint64_t i) {
      return buf[i];
    }

    void reserve(uint64_t _size);
    void resize(uint64_t _size);
  };

  // Specializations for char

  template <>
  DataBuffer<char>::~DataBuffer();

  template <>
  void DataBuffer<char>::resize(uint64_t newSize);

  // Specializations for unsigned char

  template <>
  DataBuffer<unsigned char>::~DataBuffer();

  template <>
  void DataBuffer<unsigned char>::resize(uint64_t new Size);

  #ifdef __clang__
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wweak-template-vtables"
  #endif

  extern template class DataBuffer<char>;
  extern template class DataBuffer<char*>;
  extern template class DataBuffer<double>;
  extern template class DataBuffer<int64_t>;
  extern template class DataBuffer<uint64_t>;
  extern template class DataBuffer<unsigned char>;

  #ifdef __clang__
    #pragma clang diagnostic pop
  #endif
} // namespace spark


#endif /* MEMORYPOOL_HH_ */
