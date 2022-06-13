/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#ifndef SPARK_COMPRESSION_HH
#define SPARK_COMPRESSION_HH

#include "OutputStream.hh"
#include "Common.hh"
#include "WriterOptions.hh"

namespace spark {
  /**
   * Create a compressor for the given compression kind.
   * @param kind the compression type to implement
   * @param outStream the output stream that is the underlying target
   * @param strategy compression strategy
   * @param bufferCapacity compression stream buffer total capacity
   * @param compressionBlockSize compression buffer block size
   * @param pool the memory pool
   */
  std::unique_ptr<BufferedOutputStream>
     createCompressor(CompressionKind kind,
                      OutputStream * outStream,
                      CompressionStrategy strategy,
                      uint64_t bufferCapacity,
                      uint64_t compressionBlockSize,
                      MemoryPool& pool);
}

#endif