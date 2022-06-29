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

#ifndef SPARK_WRITER_OPTIONS_HH
#define SPARK_WRITER_OPTIONS_HH

#include <memory>
#include "MemoryPool.hh"
#include "Common.hh"

namespace spark {
  // classes that hold data members so we can maintain binary compatibility
  struct WriterOptionsPrivate;

  enum CompressionStrategy {
    CompressionStrategy_SPEED = 0,
    CompressionStrategy_COMPRESSION
  };

  /**
   * Options for creating a Writer.
   */
  class WriterOptions {
    private:
      std::unique_ptr<WriterOptionsPrivate> privateBits;
    public:
      WriterOptions();
      WriterOptions(const WriterOptions&);
      WriterOptions(WriterOptions&);
      WriterOptions& operator=(const WriterOptions&);
      virtual ~WriterOptions();

      /**
       * Set the data compression block size.
       */
      WriterOptions& setCompressionBlockSize(uint64_t size);

      /**
       * Get the data compression block size.
       * @return if not set, return default value.
       */
      uint64_t getCompressionBlockSize() const;

      /**
       * Set compression kind.
       */
      WriterOptions& setCompression(CompressionKind comp);

      /**
       * Get the compression kind.
       * @return if not set, return default value which is ZLIB.
       */
      CompressionKind getCompression() const;

      /**
       * Set the compression strategy.
       */
      WriterOptions& setCompressionStrategy(CompressionStrategy strategy);

      /**
       * Get the compression strategy.
       * @return if not set, return default value which is speed.
       */
      CompressionStrategy getCompressionStrategy() const;

      /**
       * Set the memory pool.
       */
      WriterOptions& setMemoryPool(MemoryPool * memoryPool);

      /**
       * Get the memory pool.
       * @return if not set, return default memory pool.
       */
      MemoryPool * getMemoryPool() const;
  };
}

#endif