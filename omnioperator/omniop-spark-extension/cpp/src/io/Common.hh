/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description
 */

#ifndef SPARK_COMMON_HH
#define SPARK_COMMON_HH

namespace spark {

  enum CompressionKind {
    CompressionKind_NONE = 0,
    CompressionKind_ZLIB = 1,
    CompressionKind_SNAPPY = 2,
    CompressionKind_LAO = 3,
    CompressionKind_LZ4 = 4,
    CompressionKind_ZSTD = 5
  };
}

#endif