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

#include <string>
#include <utility>

#include "UriInfo.h"

static const std::string LOCAL_FILE = "file";

UriInfo::UriInfo(std::string _uri, std::string _scheme, std::string _path, std::string _host,
                 std::string _port) : hostString(std::move(_host)),
                                      schemeString(std::move(_scheme)),
                                      portString(std::move(_port)),
                                      pathString(std::move(_path)),
                                      uriString(std::move(_uri))
{
    // when local file, transfer to absolute path
    if (schemeString == LOCAL_FILE) {
        uriString = pathString;
    }
}

UriInfo::UriInfo(std::string _scheme, std::string _path, std::string _host,
                 std::string _port) : hostString(std::move(_host)),
                                      schemeString(std::move(_scheme)),
                                      portString(std::move(_port)),
                                      pathString(std::move(_path)),
                                      uriString("Not initialize origin uri!")
{
}

UriInfo::~UriInfo() {}

const std::string UriInfo::Scheme() const
{
    return schemeString;
}

const std::string UriInfo::Host() const
{
    return hostString;
}

const std::string UriInfo::Port() const
{
    return portString;
}

const std::string UriInfo::Path() const
{
    return pathString;
}

const std::string UriInfo::ToString() const
{
    return uriString;
}