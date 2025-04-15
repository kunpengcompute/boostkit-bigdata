/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef NATIVE_READER_PARQUETWRITER_H
#define NATIVE_READER_PARQUETWRITER_H



#include <vector/vector_common.h>
#include <arrow/filesystem/filesystem.h>
#include "common/UriInfo.h"
#include "parquet/arrow/writer.h"

using namespace arrow::internal;

namespace omniruntime::writer
{
    std::string get_parent_path(const std::string& path);
    int createDirectories(const std::string &path);
	class ParquetWriter
	{
	public:
		ParquetWriter() {}

		arrow::Status InitRecordWriter(UriInfo &uri, std::string &ugi);
		std::shared_ptr<arrow::Field> BuildField(const std::string &name, int typeId, bool nullable);
		void write(long *vecNativeId, int colNums, const int *omniTypes, const unsigned char *dataColumnsIds,
				   bool isSplitWrite = false, long starPos = 0, long endPos = 0);
		void write();

	public:
		std::unique_ptr<parquet::arrow::FileWriter> arrow_writer;
		std::shared_ptr<arrow::Schema> schema_;
		std::vector<int> precisions;
		std::vector<int> scales;
	};
}
#endif // NATIVE_READER_PARQUETWRITER_H