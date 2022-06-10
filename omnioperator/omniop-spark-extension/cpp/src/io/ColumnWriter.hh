/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */

#ifndef SPARK_COLUMN_WRITER_HH
#define SPARK_COLUMN_WRITER_HH

#include "Compression.hh"

namespace spark {

  class StreamsFactory {
  public:
    virtual ~StreamsFactory();

    virtual std::unique_ptr<BufferedOutputStream>
                    createStream() const = 0;
  };

  std::unique_ptr<StreamsFactory> createStreamsFactory(
                                        const WriterOptions& options,
                                        OutputStream * outStream);
}

#endif