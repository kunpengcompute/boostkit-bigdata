/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

 #ifndef CPP_BUFFER_H
 #define CPP_BUFFER_H

 #include <cstring>
 #include <vector>
 #include <chrono>
 #include <memory>
 #include <list>
 
 class Buffer {
 public:
        Buffer(uint8_t* data, int64_t size, int64_t capacity)
                : data_(data),
                size_(size),
                capacity_(capacity) {
        }

        // Unsafe methods don't check existing size
        void UnsafeAppend(const void* data, const int64_t length) {
            memcpy((void*) (data_ + size_), data, static_cast<size_t>(length));
            size_ += length;
        }

 public:
        uint8_t * data_;
        int64_t size_;
        int64_t capacity_;
 };

 int64_t BytesForBits(int64_t bits);

 #endif //CPP_BUFFER_H