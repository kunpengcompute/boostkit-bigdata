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

#include "OutputStream.hh"

#include <sstream>

namespace spark {

  BufferedOutputStream::BufferedOutputStream(
                                    MemoryPool& pool,
                                    OutputStream * outStream,
                                    uint64_t capacity_,
                                    uint64_t blockSize_)
                                    : outputStream(outStream),
                                      blockSize(blockSize_) {
    dataBuffer.reset(new DataBuffer<char>(pool));
    dataBuffer->reserve(capacity_);
  }

  BufferedOutputStream::~BufferedOutputStream() {
    // PASS
  }

  bool BufferedOutputStream::Next(void** buffer, int* size) {
    *size = static_cast<int>(blockSize);
    uint64_t oldSize = dataBuffer->size();
    uint64_t newSize = oldSize + blockSize;
    uint64_t newCapacity = dataBuffer->capacity();
    while (newCapacity < newSize) {
      newCapacity += dataBuffer->capacity();
    }
    dataBuffer->reserve(newCapacity);
    dataBuffer->resize(newSize);
    *buffer = dataBuffer->data() + oldSize;
    return true;
  }

  bool BufferedOutputStream::NextNBytes(void** buffer, int size) {
    uint64_t oldSize = dataBuffer->size();
    uint64_t newSize = oldSize + size;
    uint64_t newCapacity = dataBuffer->capacity();
    while (newCapacity < newSize) {
      newCapacity += dataBuffer->capacity();
    }
    dataBuffer->reserve(newCapacity);
    dataBuffer->resize(newSize);
    *buffer = dataBuffer->data() + oldSize;
    return true;
  }

  void BufferedOutputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= dataBuffer->size()) {
        dataBuffer->resize(dataBuffer->size() - unsignedCount);
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  google::protobuf::int64 BufferedOutputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(dataBuffer->size());
  }

  bool BufferedOutputStream::WriteAliasedRaw(const void *, int) {
    throw std::logic_error("WriteAliasedRaw is not supported.");
  }

  bool BufferedOutputStream::AllowsAliasing() const {
    return false;
  }

  std::string BufferedOutputStream::getName() const {
    std::ostringstream result;
    result << "BufferedOutputStream " << dataBuffer->size() << " of "
                                            << dataBuffer->capacity();
    return result.str();
  }

  uint64_t BufferedOutputStream::getSize() const {
    return dataBuffer->size();
  }

  uint64_t BufferedOutputStream::flush() {
    uint64_t dataSize = dataBuffer->size();
    outputStream->write(dataBuffer->data(), dataSize);
    dataBuffer->resize(0);
    return dataSize;
  }

}
