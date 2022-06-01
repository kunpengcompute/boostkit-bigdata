/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description
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
    CompressionStrategy_SPEED = 0;
    CompressionStrategy_COMPRESSION
  };

  /**
   * Options for creating a Writer.
   */
  class WriterOptions {
    private:
      std::unique_ptr<WriterOptions> privateBits;
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
       * @return if not set, return default size.
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